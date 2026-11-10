use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ::twinleaf::data::{ColumnData, SampleBatch};
use ::twinleaf::device::rpc::{RpcDescriptor, RpcMetaExt, RpcMetaFlags, RpcRegistry};
use ::twinleaf::device::{Connection, Device, RecvError, Receiver};
use ::twinleaf::proto::DeviceRoute;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

/// How long a blocked `__next__` waits before it checks for Ctrl-C.
const SIGNAL_POLL: Duration = Duration::from_millis(100);

#[pyclass(name = "_DataIterator", subclass)]
struct PyIter {
    receiver: Receiver<SampleBatch>,
    /// The batch being read, and the next row of it to read.
    batch: Option<SampleBatch>,
    row: usize,
    n: Option<usize>,
    stream: String,
    columns: Vec<String>,
}

impl PyIter {
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

    /// Returns the next row that has a matching column, or `None` at the end
    /// of the stream.
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
                match self.receiver.recv_timeout(SIGNAL_POLL) {
                    Ok(batch) => break batch,
                    Err(RecvError::Timeout) | Err(RecvError::Lagged(_)) => continue,
                    Err(_) => return Ok(None), // End of stream
                }
            };
            if !self.stream.is_empty() && self.stream != batch.schema().stream().name() {
                continue;
            }
            self.batch = Some(batch);
            self.row = 0;
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

#[pyclass(name = "_Device", subclass)]
struct PyDevice {
    root: String,
    route: DeviceRoute,
    /// Keeps the connection, and so its proxy link, open while the device lives.
    _connection: Connection,
    device: Device,
}

#[pymethods]
impl PyDevice {
    #[new]
    #[pyo3(signature = (root_url=None, route=None))]
    fn new(root_url: Option<String>, route: Option<String>) -> PyResult<PyDevice> {
        let root = root_url.unwrap_or_else(|| "tcp://localhost".to_string());
        let route = match route {
            Some(path) => DeviceRoute::from_str(&path)
                .map_err(|e| PyValueError::new_err(format!("invalid route '{path}': {e}")))?,
            None => DeviceRoute::root(),
        };
        let connection = Connection::open(&root)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to open '{root}': {e}")))?;
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
        match self.device.raw_rpc(name, req) {
            Ok(ret) => Ok(PyBytes::new(py, &ret[..])),
            _ => Err(PyRuntimeError::new_err(format!("RPC '{}' failed", name))),
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
        let readout = ::twinleaf::device::capture::read_capture(&self.device, name, timeout)
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
        let registry = self
            .device
            .rpc_registry()
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let list: Vec<_> = registry
            .iter()
            .map(|spec| (spec.full_name.clone(), spec.meta.bits()))
            .collect();
        Ok(PyList::new(py, list)?)
    }

    fn _rpc_registry(&self) -> PyResult<PyRegistry> {
        let registry = self
            .device
            .rpc_registry()
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        Ok(PyRegistry { inner: registry })
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
            batch: None,
            row: 0,
            n,
            stream: stream.unwrap_or_default(),
            columns: columns.unwrap_or_default(),
        })
    }

    fn _get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let meta = match self.device.metadata() {
            Ok(meta) => meta,
            Err(_) => return Err(PyRuntimeError::new_err("Failed to get metadata")),
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

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _twinleaf(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDevice>()?;
    m.add_class::<PyRpc>()?;
    m.add_class::<PyRegistry>()?;
    Ok(())
}
