use std::ffi::CString;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ::twinleaf::data::{ColumnData, SampleBatch};
use ::twinleaf::device::rpc::{RpcDescriptor, RpcMetaExt, RpcMetaFlags, RpcRegistry};
use ::twinleaf::device::{Connection, Device, DeviceTree, Presence, RecvError, Receiver};
use ::twinleaf::proto::DeviceRoute;
use ::twinleaf::proxy::{Layout, Mount};
use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyRuntimeWarning, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::PyTypeInfo;

/// How long a blocked `__next__` waits before it checks for Ctrl-C.
const SIGNAL_POLL: Duration = Duration::from_millis(100);

pyo3::create_exception!(
    twinleaf,
    SampleLossWarning,
    PyRuntimeWarning,
    "Samples of a stream were lost between two rows, because the reader fell behind or the device or its link dropped them."
);
pyo3::create_exception!(
    twinleaf,
    DeviceLostWarning,
    PyRuntimeWarning,
    "The device stopped responding while waiting for samples. The connection keeps trying to reach it."
);

/// Issues a Python warning of category `T`. Fails if a warnings filter turns
/// it into an error.
fn warn<T: PyTypeInfo>(py: Python<'_>, message: String) -> PyResult<()> {
    let message = CString::new(message.replace('\0', " ")).expect("NUL bytes were replaced");
    PyErr::warn(py, py.get_type::<T>().as_any(), &message, 1)
}

#[pyclass(name = "_DataIterator", subclass)]
struct PyIter {
    receiver: Receiver<SampleBatch>,
    /// Whether the device is there, as the proxy follows it.
    presence: Receiver<Presence>,
    /// Whether a `DeviceLostWarning` was issued since the device last sent data.
    device_lost: bool,
    route: String,
    /// The batch being read, and the next row of it to read.
    batch: Option<SampleBatch>,
    row: usize,
    n: Option<usize>,
    stream: String,
    columns: Vec<String>,
}

impl PyIter {
    /// Warns once when the proxy reports the device lost. Called while no
    /// batch arrives, so a lost device does not go unnoticed during the time
    /// the connection spends reconnecting.
    fn watch_presence(&mut self, py: Python<'_>) -> PyResult<()> {
        loop {
            match self.presence.try_recv() {
                Ok(Some(Presence::Lost { since })) => {
                    if !self.device_lost {
                        self.device_lost = true;
                        warn::<DeviceLostWarning>(
                            py,
                            format!(
                                "the device at {} stopped responding {:.1} s ago; waiting for it to reconnect",
                                self.route,
                                since.elapsed().as_secs_f64()
                            ),
                        )?;
                    }
                }
                Ok(Some(_)) => {}
                Err(RecvError::Lagged(_)) => {}
                // No change pending. An ended connection is reported by the
                // sample receiver.
                Ok(None) | Err(_) => return Ok(()),
            }
        }
    }

    /// Warns if samples of the batch's stream were lost before it.
    fn warn_loss(&self, py: Python<'_>, batch: &SampleBatch) -> PyResult<()> {
        let Some(loss) = batch.boundary().and_then(|b| b.loss.as_ref()) else {
            return Ok(());
        };
        let cause = if loss.dropped_batches > 0 {
            format!(
                "this reader fell behind, and {} batches were dropped from its queue",
                loss.dropped_batches
            )
        } else {
            "the device or its link dropped them".to_string()
        };
        warn::<SampleLossWarning>(
            py,
            format!(
                "stream '{}' lost {} samples: {cause}",
                batch.schema().stream().name(),
                loss.samples
            ),
        )
    }

    fn column_matches(&self, name: &str) -> bool {
        self.columns.is_empty()
            || self.columns.iter().any(|c| {
                if let Some(prefix) = c.strip_suffix('*') {
                    name.starts_with(prefix)
                } else {
                    c == name
                }
            })
    }

    /// Builds the dict for one row, or `None` if no column of the row matches.
    fn row_dict(&self, py: Python<'_>, batch: &SampleBatch, row: usize) -> PyResult<Option<Py<PyDict>>> {
        let Some(sample) = batch.row(row) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        for (column, value) in batch.schema().columns().zip(sample.values()) {
            let name = column.name();
            if !self.column_matches(name) {
                continue;
            }
            if dict.is_empty() {
                let stream_id = batch.schema().stream().stream_id().value();
                dict.set_item("stream", stream_id)?;
                dict.set_item("time", sample.timestamp_end())?;
            }
            dict.set_item(name, column_value(py, value)?)?;
        }
        Ok(if dict.is_empty() { None } else { Some(dict.unbind()) })
    }

    /// Returns the next row that has a matching column. Raises
    /// `ConnectionError` if the connection to the proxy ends.
    fn next_dict(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        loop {
            if let Some(batch) = self.batch.take() {
                while self.row < batch.len() {
                    let row = self.row;
                    self.row += 1;
                    if let Some(dict) = self.row_dict(py, &batch, row)? {
                        self.batch = Some(batch);
                        return Ok(Some(dict));
                    }
                }
            }

            let batch = loop {
                py.check_signals()?;
                // Release the GIL while waiting so other Python threads run.
                let receiver = &self.receiver;
                match py.detach(|| receiver.recv_timeout(SIGNAL_POLL)) {
                    Ok(batch) => break batch,
                    Err(RecvError::Timeout) => self.watch_presence(py)?,
                    // The next batch of each stream that lost batches names
                    // the loss, and warn_loss reports it.
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Stopped) => {
                        return Err(PyConnectionError::new_err(format!(
                            "the tio proxy stopped, which ended the samples from {}",
                            self.route
                        )))
                    }
                    Err(error) => {
                        return Err(PyConnectionError::new_err(format!(
                            "lost the connection to the tio proxy, which ended the samples from {}: {error}",
                            self.route
                        )))
                    }
                }
            };
            // Data is arriving, so the device is there again.
            self.device_lost = false;
            if !self.stream.is_empty() && self.stream != batch.schema().stream().name() {
                continue;
            }
            // Keep the batch before warning, so iteration can resume if a
            // warnings filter raises the warning as an error.
            self.batch = Some(batch.clone());
            self.row = 0;
            self.warn_loss(py, &batch)?;
        }
    }
}

/// Converts one decoded value to the Python type its column type maps to.
fn column_value<'py>(py: Python<'py>, value: ColumnData) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        ColumnData::I8(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::I16(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::I32(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::I64(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::U8(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::U16(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::U32(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::U64(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::F32(x) => f64::from(x).into_pyobject(py)?.into_any(),
        ColumnData::F64(x) => x.into_pyobject(py)?.into_any(),
        ColumnData::Unknown => "UNKNOWN".into_pyobject(py)?.into_any(),
    })
}

#[pymethods]
impl PyIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        // Check if we have a finite count and if it's reached zero
        if let Some(ctr) = slf.n {
            if ctr == 0 {
                return Ok(None);
            } else {
                slf.n = Some(ctr - 1);
            }
        }
        // If n is None, we continue indefinitely

        let py = slf.py();
        Ok(slf.next_dict(py)?.map(|dict| dict.into_any()))
    }
}

#[pyclass(name = "_Rpc")]
struct PyRpc {
    inner: RpcDescriptor,
}

#[pymethods]
impl PyRpc {
    #[getter]
    fn name(&self) -> String {
        self.inner.full_name.clone()
    }

    #[getter]
    fn readable(&self) -> bool {
        self.inner.meta.flags().contains(RpcMetaFlags::READABLE)
    }

    #[getter]
    fn writable(&self) -> bool {
        self.inner.meta.flags().contains(RpcMetaFlags::WRITABLE)
    }

    #[getter]
    fn size_bytes(&self) -> Option<usize> {
        self.inner.meta.size_bytes()
    }

    #[getter]
    fn type_str(&self) -> String {
        self.inner.meta.type_str()
    }

    #[getter]
    fn is_capture(&self) -> bool {
        self.inner.meta.flags().contains(RpcMetaFlags::CAPTURE)
    }

    #[getter]
    fn meta_raw(&self) -> u16 {
        self.inner.meta.bits()
    }

    fn __repr__(&self) -> String {
        format!(
            "_twinleaf._Rpc({} {}({}))",
            self.inner.meta.perm_str(),
            self.inner.full_name,
            self.inner.meta.type_str(),
        )
    }
}

#[pyclass(name = "_RpcRegistry")]
struct PyRegistry {
    inner: Arc<RpcRegistry>,
}

#[pymethods]
impl PyRegistry {
    /// The names directly below `prefix`, sorted and without repeats.
    fn children_of(&self, prefix: &str) -> Vec<String> {
        let lo = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}.")
        };
        let mut out: Vec<String> = Vec::new();
        for name in self.inner.names() {
            let Some(rest) = name.strip_prefix(&lo) else {
                continue;
            };
            let seg = rest.split('.').next().unwrap_or(rest);
            if out.last().map(String::as_str) != Some(seg) {
                out.push(seg.to_string());
            }
        }
        out
    }

    fn find(&self, name: &str) -> Option<PyRpc> {
        self.inner.find(name).map(|desc| PyRpc { inner: desc.clone() })
    }

    fn suggest(&self, query: &str) -> Vec<String> {
        self.inner
            .iter()
            .map(|desc| desc.full_name.clone())
            .filter(|name| name.starts_with(query))
            .collect()
    }

    fn search(&self, query: &str) -> Vec<String> {
        self.inner
            .iter()
            .map(|desc| desc.full_name.clone())
            .filter(|name| name.contains(query))
            .collect()
    }

    #[getter]
    fn hash(&self) -> Option<u32> {
        self.inner.hash
    }

    fn __repr__(&self) -> String {
        format!("_twinleaf._RpcRegistry({:?})", self.children_of(""))
    }
}

/// Parses a route such as `/0/1`, `0/1` or `/`.
fn parse_route(path: &str) -> PyResult<DeviceRoute> {
    DeviceRoute::from_str(path)
        .map_err(|e| PyValueError::new_err(format!("invalid route '{path}': {e}")))
}

/// Opens the default device when `urls` is `None` or empty, the device at
/// the one URL given, or every URL given mounted as one tree at `/0`, `/1`,
/// and so on. Returns a label for the connection and the connection.
fn open_connection(py: Python<'_>, urls: Option<Vec<String>>) -> PyResult<(String, Connection)> {
    let urls = urls.unwrap_or_default();
    let (label, opened) = match urls.as_slice() {
        [] => ("auto".to_string(), py.detach(Connection::connect)),
        [url] => (url.clone(), py.detach(|| Connection::open(url))),
        _ => {
            let mut mounts = Vec::with_capacity(urls.len());
            for (index, url) in urls.iter().enumerate() {
                let hop = u8::try_from(index)
                    .map_err(|_| PyValueError::new_err("at most 256 URLs can be mounted"))?;
                mounts.push(Mount {
                    url: url.clone(),
                    source: DeviceRoute::root(),
                    prefix: DeviceRoute::from_hops(&[hop])
                        .map_err(|e| PyValueError::new_err(format!("{e}")))?,
                });
            }
            let layout = Layout { mounts };
            (urls.join(", "), py.detach(|| Connection::open_layout(&layout)))
        }
    };
    let connection =
        opened.map_err(|e| PyRuntimeError::new_err(format!("failed to open '{label}': {e}")))?;
    Ok((label, connection))
}

/// One connection to a whole device tree, which every device of the tree
/// shares.
#[pyclass(name = "_Tree")]
struct PyTree {
    label: String,
    connection: Connection,
    tree: DeviceTree,
}

#[pymethods]
impl PyTree {
    #[new]
    #[pyo3(signature = (urls=None))]
    fn new(py: Python<'_>, urls: Option<Vec<String>>) -> PyResult<PyTree> {
        let (label, connection) = open_connection(py, urls)?;
        let tree = connection.tree(DeviceRoute::root());
        Ok(PyTree { label, connection, tree })
    }

    #[getter]
    fn _url(&self) -> String {
        self.label.clone()
    }

    /// Listens for `window_seconds` and returns every route the proxy has
    /// heard, each with its `dev.name` and `dev.serial`, or `None` where the
    /// device did not answer. A device whose `dev.serial` is blank gets the
    /// serial from its metadata instead, which the proxy has usually
    /// collected already.
    fn _discover(
        &self,
        py: Python<'_>,
        window_seconds: f64,
    ) -> PyResult<Vec<(String, Option<String>, Option<String>)>> {
        if !window_seconds.is_finite() || window_seconds < 0.0 {
            return Err(PyValueError::new_err(
                "discovery window must be a non-negative finite number of seconds",
            ));
        }
        let window = Duration::from_secs_f64(window_seconds);
        let tree = &self.tree;
        Ok(py.detach(|| {
            let routes = tree.discover_routes(window);
            // Send every call before waiting on any, so the devices answer in
            // parallel.
            let pending: Vec<_> = routes
                .iter()
                .map(|route| {
                    (
                        tree.submit(route.clone(), "dev.name", &[]),
                        tree.submit(route.clone(), "dev.serial", &[]),
                    )
                })
                .collect();
            let text = |reply: Result<Vec<u8>, _>| {
                reply
                    .ok()
                    .map(|bytes: Vec<u8>| String::from_utf8_lossy(&bytes).trim().to_string())
                    .filter(|text| !text.is_empty())
            };
            routes
                .iter()
                .zip(pending)
                .map(|(route, (name, serial))| {
                    let serial = text(serial.wait()).or_else(|| {
                        tree.metadata(route.clone())
                            .ok()
                            .map(|meta| meta.device().serial().trim().to_string())
                            .filter(|serial| !serial.is_empty())
                    });
                    (route.to_string(), text(name.wait()), serial)
                })
                .collect()
        }))
    }
}

#[pyclass(name = "_Device", subclass)]
struct PyDevice {
    root: String,
    route: DeviceRoute,
    /// Keeps the connection, and so its proxy link, open while the device lives.
    _connection: Connection,
    device: Device,
}

impl PyDevice {
    /// Loads the device's RPC table, with the GIL released while it waits.
    fn load_registry(&self, py: Python<'_>) -> PyResult<Arc<RpcRegistry>> {
        let device = &self.device;
        py.detach(|| device.rpc_registry())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to load the RPC table: {e}")))
    }
}

#[pymethods]
impl PyDevice {
    #[new]
    /// Binds `route` on `tree`'s connection when a tree is given, or else
    /// opens a connection of its own to `root_url`, or to the default device
    /// when that is `None`.
    #[pyo3(signature = (root_url=None, route=None, tree=None))]
    fn new(
        py: Python<'_>,
        root_url: Option<String>,
        route: Option<String>,
        tree: Option<PyRef<'_, PyTree>>,
    ) -> PyResult<PyDevice> {
        let route = match route {
            Some(path) => parse_route(&path)?,
            None => DeviceRoute::root(),
        };
        let (root, connection) = match tree {
            Some(tree) => (tree.label.clone(), tree.connection.clone()),
            None => open_connection(py, root_url.map(|url| vec![url]))?,
        };
        let device = connection.device(route.clone());
        Ok(PyDevice { root, route, _connection: connection, device })
    }

    #[getter]
    fn _url(&self) -> String {
        self.root.clone()
    }

    #[getter]
    fn _route(&self) -> String {
        format!("{}", self.route)
    }

    fn _rpc<'py>(&self, py: Python<'py>, name: &str, req: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
        let device = &self.device;
        match py.detach(|| device.raw_rpc(name, req)) {
            Ok(ret) => Ok(PyBytes::new(py, &ret[..])),
            Err(error) => Err(PyRuntimeError::new_err(format!("RPC '{name}' failed: {error}"))),
        }
    }

    #[pyo3(signature = (name, timeout_seconds=5.0, raw=false))]
    fn _capture<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        timeout_seconds: f64,
        raw: bool,
    ) -> PyResult<Py<PyAny>> {
        if !timeout_seconds.is_finite() || timeout_seconds < 0.0 {
            return Err(PyValueError::new_err(
                "capture timeout must be a non-negative finite number of seconds",
            ));
        }

        let timeout = Duration::from_secs_f64(timeout_seconds);
        let device = &self.device;
        let readout = py
            .detach(|| ::twinleaf::device::capture::read_capture(device, name, timeout))
            .map_err(|err| {
                PyRuntimeError::new_err(format!("Capture RPC '{}' failed: {}", name, err))
            })?;
        let values = readout
            .values_f64()
            .map_err(|err| {
                PyRuntimeError::new_err(format!("Capture RPC '{}' failed: {}", name, err))
            })?;
        let x_values: Vec<f64> = (0..values.len())
            .map(|index| readout.metadata.x_value_f64(index))
            .collect();
        let meta = &readout.metadata;

        let dict = PyDict::new(py);
        if raw {
            dict.set_item("size", meta.size)?;
            dict.set_item("blocksize", meta.blocksize)?;
            dict.set_item("data_type", meta.data_type_label())?;
            dict.set_item("length", meta.length)?;
            dict.set_item("y_calibration", meta.y_calibration)?;
            dict.set_item("x_offset", meta.x_offset)?;
            dict.set_item("x_stride", meta.x_stride)?;
            dict.set_item("data", PyBytes::new(py, &readout.data))?;
        }
        dict.set_item("name", &meta.name)?;
        dict.set_item("units", &meta.units)?;
        dict.set_item("x_name", &meta.x_name)?;
        dict.set_item("x_units", &meta.x_units)?;
        dict.set_item("x", x_values)?;
        dict.set_item("y", values)?;

        Ok(dict.into())
    }

    fn _rpc_list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let registry = self.load_registry(py)?;
        let list: Vec<_> = registry
            .iter()
            .map(|spec| (spec.full_name.clone(), spec.meta.bits()))
            .collect();
        Ok(PyList::new(py, list)?)
    }

    fn _rpc_registry(&self, py: Python<'_>) -> PyResult<PyRegistry> {
        Ok(PyRegistry { inner: self.load_registry(py)? })
    }

    #[pyo3(signature = (n=None, stream=None, columns=None))]
    fn _samples<'py>(
        &self,
        _py: Python<'py>,
        n: Option<usize>,
        stream: Option<String>,
        columns: Option<Vec<String>>,
    ) -> PyResult<PyIter> {
        Ok(PyIter {
            receiver: self.device.samples(),
            presence: self.device.presence_updates(),
            device_lost: false,
            route: self.route.to_string(),
            batch: None,
            row: 0,
            n,
            stream: stream.unwrap_or_default(),
            columns: columns.unwrap_or_default(),
        })
    }

    fn _get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let device = &self.device;
        let meta = match py.detach(|| device.metadata()) {
            Ok(meta) => meta,
            Err(error) => {
                return Err(PyRuntimeError::new_err(format!("Failed to get metadata: {error}")))
            }
        };

        let dict = PyDict::new(py);

        // Convert device metadata to dict
        let device = meta.device();
        let device_dict = PyDict::new(py);
        device_dict.set_item("serial_number", device.serial().to_string())?;
        device_dict.set_item("firmware_hash", device.firmware().to_string())?;
        device_dict.set_item("session_id", device.session().to_string())?;
        device_dict.set_item("name", device.name().to_string())?;
        dict.set_item("device", device_dict)?;

        // Convert streams to dict
        let streams_dict = PyDict::new(py);
        for (stream_id, stream) in meta.streams() {
            let schema = stream.schema();
            let stream_view = schema.stream();
            let stream_dict = PyDict::new(py);
            stream_dict.set_item("stream_id", stream_id.to_string())?;

            let columns_dict = PyDict::new(py);
            for col in schema.columns() {
                let col_dict = PyDict::new(py);
                col_dict.set_item("name", col.name())?;
                col_dict.set_item("description", col.description())?;
                col_dict.set_item("type", col.data_type().to_string())?;
                col_dict.set_item("units", col.units())?;

                columns_dict.set_item(col.name(), col_dict)?;
            }
            stream_dict.set_item("columns", columns_dict)?;
            streams_dict.set_item(stream_view.name(), stream_dict)?;
        }
        dict.set_item("streams", streams_dict)?;

        Ok(dict.into())
    }
}

/// Returns `route` in canonical form, such as `/0/1`. Raises `ValueError`
/// for a malformed route.
#[pyfunction]
fn _parse_route(route: &str) -> PyResult<String> {
    Ok(parse_route(route)?.to_string())
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _twinleaf(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTree>()?;
    m.add_class::<PyDevice>()?;
    m.add_function(wrap_pyfunction!(_parse_route, m)?)?;
    m.add_class::<PyRpc>()?;
    m.add_class::<PyRegistry>()?;
    m.add("SampleLossWarning", m.py().get_type::<SampleLossWarning>())?;
    m.add("DeviceLostWarning", m.py().get_type::<DeviceLostWarning>())?;
    Ok(())
}
