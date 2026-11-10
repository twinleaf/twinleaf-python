//! A tree-wide acquisition: one subscription to every route, drained by a
//! background thread into a ring buffer per stream, from which Python cuts
//! time slices. All streams of a slice therefore come from one subscription
//! over the same interval.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ::twinleaf::data::{Change, ColumnData, SampleBatch, StreamMetadataSnapshot};
use ::twinleaf::device::{DeviceTree, RecvError};
use ::twinleaf::proto::DeviceRoute;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

/// How long the drain thread waits for a batch before checking for a stop.
const DRAIN_POLL: Duration = Duration::from_millis(100);
/// The most rows a stream keeps, whatever its retention.
const MAX_ROWS: usize = 20_000_000;

/// The name of a stream, as `/route/stream`: `/imu` at the root, `/0/vector`
/// at route `/0`.
fn stream_path(route: &DeviceRoute, stream: &str) -> String {
    let route = route.to_string();
    if route == "/" {
        format!("/{stream}")
    } else {
        format!("{route}/{stream}")
    }
}

/// The identity of the clock a stream's timestamps count on. Streams can be
/// compared by time only when these are equal.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Timebase {
    epoch: String,
    serial: String,
    session: u32,
}

impl Timebase {
    fn of(meta: &StreamMetadataSnapshot) -> Timebase {
        let segment = meta.segment();
        Timebase {
            epoch: segment.epoch().to_string(),
            serial: segment.timeref_serial().to_string(),
            session: segment.timeref_session().value(),
        }
    }
}

/// One column's retained values, as little-endian bytes of a fixed-size
/// numpy dtype.
struct ColumnBuf {
    name: String,
    units: String,
    /// numpy dtype string, such as `<f4`, set by the first value.
    dtype: Option<&'static str>,
    size: usize,
    bytes: Vec<u8>,
}

impl ColumnBuf {
    fn new(name: &str, units: &str) -> ColumnBuf {
        ColumnBuf {
            name: name.to_string(),
            units: units.to_string(),
            dtype: None,
            size: 0,
            bytes: Vec::new(),
        }
    }

    /// Appends one value. Returns false if its type differs from the
    /// column's, which only a change of schema causes.
    fn push(&mut self, value: ColumnData) -> bool {
        let (dtype, bytes): (&'static str, Vec<u8>) = match value {
            ColumnData::I8(x) => ("i1", x.to_le_bytes().to_vec()),
            ColumnData::U8(x) => ("u1", x.to_le_bytes().to_vec()),
            ColumnData::I16(x) => ("<i2", x.to_le_bytes().to_vec()),
            ColumnData::U16(x) => ("<u2", x.to_le_bytes().to_vec()),
            ColumnData::I32(x) => ("<i4", x.to_le_bytes().to_vec()),
            ColumnData::U32(x) => ("<u4", x.to_le_bytes().to_vec()),
            ColumnData::I64(x) => ("<i8", x.to_le_bytes().to_vec()),
            ColumnData::U64(x) => ("<u8", x.to_le_bytes().to_vec()),
            ColumnData::F32(x) => ("<f4", x.to_le_bytes().to_vec()),
            ColumnData::F64(x) => ("<f8", x.to_le_bytes().to_vec()),
            _ => ("<f8", f64::NAN.to_le_bytes().to_vec()),
        };
        match self.dtype {
            None => {
                self.dtype = Some(dtype);
                self.size = bytes.len();
            }
            Some(current) if current != dtype => return false,
            Some(_) => {}
        }
        self.bytes.extend_from_slice(&bytes);
        true
    }
}

/// Samples a receiver did not get, between two retained rows.
#[derive(Clone)]
struct Gap {
    id: u64,
    after: f64,
    before: f64,
    samples: u64,
    dropped_batches: u64,
}

/// One stream's retained rows of its current run.
struct Ring {
    route: DeviceRoute,
    stream: String,
    timebase: Option<Timebase>,
    rate_hz: f64,
    time: Vec<f64>,
    columns: Vec<ColumnBuf>,
    gaps: VecDeque<Gap>,
    /// Why the current run began, when it began at a break, and its time.
    run_break: Option<(f64, String)>,
    /// host UNIX time minus device time, per batch, with the batch's last
    /// device time.
    host_offsets: VecDeque<(f64, f64)>,
}

impl Ring {
    fn new(route: DeviceRoute, stream: &str) -> Ring {
        Ring {
            route,
            stream: stream.to_string(),
            timebase: None,
            rate_hz: f64::NAN,
            time: Vec::new(),
            columns: Vec::new(),
            gaps: VecDeque::new(),
            run_break: None,
            host_offsets: VecDeque::new(),
        }
    }

    /// Takes the stream's description from its metadata, before any data.
    fn describe(&mut self, meta: &StreamMetadataSnapshot) {
        self.timebase = Some(Timebase::of(meta));
        self.rate_hz = meta.rate_hz();
        self.columns = meta
            .schema()
            .columns()
            .map(|column| ColumnBuf::new(column.name(), column.units()))
            .collect();
    }

    /// Drops every retained row, starting a new run that began at `at` for
    /// `reason`.
    fn start_run(&mut self, at: f64, reason: String) {
        self.time.clear();
        for column in &mut self.columns {
            column.bytes.clear();
            column.dtype = None;
        }
        self.gaps.clear();
        self.host_offsets.clear();
        self.run_break = Some((at, reason));
    }

    fn period(&self) -> f64 {
        if self.rate_hz > 0.0 {
            1.0 / self.rate_hz
        } else {
            0.0
        }
    }

    /// The start of the first retained sample's interval.
    fn first_begin(&self) -> Option<f64> {
        self.time.first().map(|first| first - self.period())
    }

    /// Drops rows older than `retention` seconds before the newest row.
    fn prune(&mut self, retention: f64) {
        let Some(&last) = self.time.last() else {
            return;
        };
        let cutoff = last - retention;
        let mut drop = self.time.partition_point(|&t| t < cutoff);
        if self.time.len() - drop > MAX_ROWS {
            drop = self.time.len() - MAX_ROWS;
        }
        // Drain in chunks, so that each row moves a bounded number of times.
        if drop == 0 || (drop < 4096 && drop * 4 < self.time.len()) {
            return;
        }
        self.time.drain(..drop);
        for column in &mut self.columns {
            column.bytes.drain(..drop * column.size);
        }
        while self.gaps.front().is_some_and(|gap| gap.before < cutoff) {
            self.gaps.pop_front();
        }
        while self.host_offsets.front().is_some_and(|(t, _)| *t < cutoff) {
            self.host_offsets.pop_front();
        }
    }
}

/// Everything the drain thread shares with Python.
struct State {
    rings: BTreeMap<String, Ring>,
    /// Streams to keep, or every stream when `None`.
    only: Option<BTreeSet<String>>,
    retention: f64,
    next_gap: u64,
    lagged: u64,
    /// Why the subscription ended, once it has.
    ended: Option<String>,
}

impl State {
    fn process(&mut self, batch: &SampleBatch, host_now: f64) {
        let route = batch.route();
        let schema = batch.schema();
        let stream = schema.stream().name().to_string();
        let path = stream_path(&route, &stream);
        if self.only.as_ref().is_some_and(|only| !only.contains(&path)) {
            return;
        }
        let times = batch.timestamps();
        let Some(&first_time) = times.first() else {
            return;
        };
        let ring = self
            .rings
            .entry(path)
            .or_insert_with(|| Ring::new(route.clone(), &stream));

        let meta = batch.metadata();
        let timebase = Timebase::of(meta);
        let names: Vec<&str> = schema.columns().map(|column| column.name()).collect();
        let same_columns = ring.columns.len() == names.len()
            && ring.columns.iter().zip(&names).all(|(column, name)| column.name == *name);

        // A break in the stream's time axis ends its run: older rows cannot be
        // compared with newer ones.
        let change = batch.boundary().and_then(|boundary| boundary.change.clone());
        let reason = match change {
            Some(Change::SessionChanged { .. }) => Some("the device restarted".to_string()),
            Some(Change::Restarted { .. }) => Some("the stream restarted".to_string()),
            Some(Change::TimeRefSessionChanged { .. }) => {
                Some("its time reference changed".to_string())
            }
            Some(Change::TimeBackward { gap_seconds }) => {
                Some(format!("its time went back {gap_seconds:.3} s"))
            }
            _ => None,
        };
        let reason = reason.or_else(|| {
            if !ring.time.is_empty() && ring.timebase.as_ref().is_some_and(|known| *known != timebase)
            {
                Some("its time reference changed".to_string())
            } else if !ring.time.is_empty() && !same_columns {
                Some("its columns changed".to_string())
            } else if ring.time.last().is_some_and(|&last| first_time <= last) {
                Some("its time went back".to_string())
            } else {
                None
            }
        });
        if let Some(reason) = reason {
            ring.start_run(first_time, reason);
        }
        if ring.time.is_empty() {
            ring.describe(meta);
        }
        ring.timebase = Some(timebase);
        ring.rate_hz = meta.rate_hz();

        if let Some(loss) = batch.boundary().and_then(|boundary| boundary.loss.as_ref()) {
            if let Some(&after) = ring.time.last() {
                ring.gaps.push_back(Gap {
                    id: self.next_gap,
                    after,
                    before: first_time,
                    samples: loss.samples,
                    dropped_batches: loss.dropped_batches,
                });
                self.next_gap += 1;
            }
        }

        for (row, &time) in times.iter().enumerate() {
            for (column, values) in ring.columns.iter_mut().zip(batch.columns()) {
                if !column.push(values.get(row)) {
                    // A value of another type: the schema changed without a
                    // boundary. Start over from this batch.
                    ring.start_run(first_time, "its column types changed".to_string());
                    return;
                }
            }
            ring.time.push(time);
        }
        if let Some(&last) = times.last() {
            ring.host_offsets.push_back((last, host_now - last));
        }
        ring.prune(self.retention);
    }
}

fn host_unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64())
        .unwrap_or(f64::NAN)
}

/// Buffers every stream of a tree from the moment it starts.
#[pyclass(name = "_Acquisition")]
pub struct PyAcquisition {
    state: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl PyAcquisition {
    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic in the drain thread poisons the lock; the state stays usable.
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Starts buffering `tree`'s streams, or only the paths in `streams`.
    /// The streams of every route in `routes` are described from their
    /// metadata up front, so they are known before their first batch.
    pub fn start(
        py: Python<'_>,
        tree: &DeviceTree,
        routes: Vec<String>,
        streams: Option<Vec<String>>,
        retention: f64,
    ) -> PyResult<PyAcquisition> {
        if !retention.is_finite() || retention <= 0.0 {
            return Err(PyValueError::new_err(
                "retention must be a positive finite number of seconds",
            ));
        }
        let routes = routes
            .iter()
            .map(|route| crate::parse_route(route))
            .collect::<PyResult<Vec<_>>>()?;
        // Subscribe first, so nothing between describing and draining is
        // missed.
        let receiver = tree.samples();
        let described = py.detach(|| {
            let mut rings = BTreeMap::new();
            for route in &routes {
                if let Ok(meta) = tree.metadata(route.clone()) {
                    for (_, stream) in meta.streams() {
                        let name = stream.schema().stream().name().to_string();
                        let mut ring = Ring::new(route.clone(), &name);
                        ring.describe(stream);
                        rings.insert(stream_path(route, &name), ring);
                    }
                }
            }
            rings
        });

        let only = streams.map(|paths| paths.into_iter().collect::<BTreeSet<_>>());
        if let Some(only) = &only {
            let unknown: Vec<&String> =
                only.iter().filter(|path| !described.contains_key(*path)).collect();
            if !unknown.is_empty() {
                return Err(PyKeyError::new_err(format!(
                    "no stream {unknown:?} in this tree; its streams are {:?}",
                    described.keys().collect::<Vec<_>>()
                )));
            }
        }
        let rings = match &only {
            Some(only) => described
                .into_iter()
                .filter(|(path, _)| only.contains(path))
                .collect(),
            None => described,
        };
        let state = Arc::new(Mutex::new(State {
            rings,
            only,
            retention,
            next_gap: 0,
            lagged: 0,
            ended: None,
        }));
        let stop = Arc::new(AtomicBool::new(false));

        let thread = {
            let state = state.clone();
            let stop = stop.clone();
            std::thread::Builder::new()
                .name("twinleaf-acquisition".to_string())
                .spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let received = receiver.recv_timeout(DRAIN_POLL);
                        let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
                        match received {
                            Ok(batch) => state.process(&batch, host_unix_now()),
                            Err(RecvError::Timeout) => {}
                            Err(RecvError::Lagged(count)) => state.lagged += count,
                            Err(RecvError::Stopped) => {
                                state.ended = Some("the tio proxy stopped".to_string());
                                return;
                            }
                            Err(error) => {
                                state.ended =
                                    Some(format!("lost the connection to the tio proxy: {error}"));
                                return;
                            }
                        }
                    }
                })
                .map_err(|e| PyRuntimeError::new_err(format!("cannot start acquisition: {e}")))?
        };
        Ok(PyAcquisition { state, stop, thread: Some(thread) })
    }
}

#[pymethods]
impl PyAcquisition {
    /// Describes every stream: its path, route, name, timebase, rate,
    /// columns, rows retained, and the interval they cover.
    fn _streams<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let state = self.lock();
        let list = PyList::empty(py);
        for (path, ring) in &state.rings {
            let info = PyDict::new(py);
            info.set_item("path", path)?;
            info.set_item("route", ring.route.to_string())?;
            info.set_item("stream", &ring.stream)?;
            match &ring.timebase {
                Some(tb) => info.set_item("timebase", (&tb.epoch, &tb.serial, tb.session))?,
                None => info.set_item("timebase", py.None())?,
            }
            info.set_item("rate_hz", ring.rate_hz)?;
            info.set_item("rows", ring.time.len())?;
            info.set_item("first_begin", ring.first_begin())?;
            info.set_item("last_end", ring.time.last().copied())?;
            let columns: Vec<(&str, &str)> = ring
                .columns
                .iter()
                .map(|column| (column.name.as_str(), column.units.as_str()))
                .collect();
            info.set_item("columns", columns)?;
            info.set_item("run_break", ring.run_break.clone())?;
            let offset = ring
                .host_offsets
                .iter()
                .map(|(_, offset)| *offset)
                .fold(f64::INFINITY, f64::min);
            info.set_item("host_offset", offset.is_finite().then_some(offset))?;
            list.append(info)?;
        }
        Ok(list)
    }

    /// Why the subscription ended, or `None` while it runs, and how many
    /// batches the queue dropped.
    fn _status(&self) -> (Option<String>, u64) {
        let state = self.lock();
        (state.ended.clone(), state.lagged)
    }

    /// Returns the rows of `path` whose end-of-sample time lies in
    /// `(start, end]`: their times and each column as little-endian bytes
    /// with a numpy dtype, and the gaps among them.
    fn _slice<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        start: f64,
        end: f64,
    ) -> PyResult<Bound<'py, PyDict>> {
        let state = self.lock();
        let ring = state
            .rings
            .get(path)
            .ok_or_else(|| PyKeyError::new_err(path.to_string()))?;
        let lo = ring.time.partition_point(|&t| t <= start);
        let hi = ring.time.partition_point(|&t| t <= end);
        let out = PyDict::new(py);
        let time: Vec<u8> = ring.time[lo..hi].iter().flat_map(|t| t.to_le_bytes()).collect();
        out.set_item("time", PyBytes::new(py, &time))?;
        let columns = PyList::empty(py);
        for column in &ring.columns {
            let dtype = column.dtype.unwrap_or("<f8");
            let bytes = &column.bytes[lo * column.size..hi * column.size];
            columns.append((&column.name, dtype, &column.units, PyBytes::new(py, bytes)))?;
        }
        out.set_item("columns", columns)?;
        let gaps: Vec<(u64, f64, f64, u64, u64)> = ring
            .gaps
            .iter()
            .filter(|gap| gap.before > start && gap.after < end)
            .map(|gap| (gap.id, gap.after, gap.before, gap.samples, gap.dropped_batches))
            .collect();
        out.set_item("gaps", gaps)?;
        Ok(out)
    }

    /// Stops buffering. The retained rows stay readable.
    fn _close(&mut self, py: Python<'_>) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            py.detach(|| {
                let _ = thread.join();
            });
        }
    }
}

impl Drop for PyAcquisition {
    fn drop(&mut self) {
        // The thread sees the flag within DRAIN_POLL and exits on its own.
        self.stop.store(true, Ordering::Relaxed);
    }
}
