use std::time::{Duration, Instant};

use ::twinleaf::tio::*;
use ::twinleaf::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

#[pyclass(name = "_DataIterator", subclass)]
struct PyIter {
    tree: device::DeviceTree,
    n: Option<usize>,
    stream: String,
    columns: Vec<String>,
    /// Batch currently being drained, one row per `__next__` call.
    batch: Option<data::SampleBatch>,
    row: usize,
    /// Which of the batch's schema columns pass the column filter.
    matched: Vec<bool>,
}

fn column_mask(schema: &[data::Series], filters: &[String]) -> Vec<bool> {
    schema
        .iter()
        .map(|series| {
            let name = &series.metadata().name;
            filters.is_empty()
                || filters.iter().any(|c| {
                    if let Some(prefix) = c.strip_suffix('*') {
                        name.starts_with(prefix)
                    } else {
                        c == name
                    }
                })
        })
        .collect()
}

impl PyIter {
    /// Block until a batch for the requested stream arrives. The GIL is
    /// released while waiting so other Python threads keep running, and
    /// signals are checked on every wakeup (at least every 100ms) so Ctrl-C
    /// interrupts the wait even when filtered-out batches keep arriving.
    fn wait_batch(&mut self, py: Python<'_>) -> PyResult<data::SampleBatch> {
        loop {
            py.check_signals()?;
            let deadline = Instant::now() + Duration::from_millis(100);
            let tree = &mut self.tree;
            match py.detach(move || tree.recv_deadline(deadline)) {
                Ok(device::TreeItem::Batch(batch)) => {
                    if self.stream.is_empty() || self.stream == batch.stream().name {
                        return Ok(batch);
                    }
                }
                Ok(device::TreeItem::Event(device::TreeEvent::Device {
                    event: device::DeviceEvent::Status(status),
                    ..
                })) => match status {
                    proto::ProxyStatus::FailedToConnect | proto::ProxyStatus::FailedToReconnect => {
                        return Err(PyRuntimeError::new_err("failed to connect to device"))
                    }
                    // A transient disconnect resolves as a reconnect, which
                    // surfaces as a boundary on the next batch.
                    _ => {}
                },
                Ok(device::TreeItem::Event(_)) => {}
                Err(proxy::RecvTimeoutError::Timeout) => {}
                Err(proxy::RecvTimeoutError::ProxyDisconnected) => {
                    return Err(PyRuntimeError::new_err("proxy connection closed"))
                }
            }
        }
    }
}

#[pymethods]
impl PyIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        py.check_signals()?;

        if slf.n == Some(0) {
            return Ok(None);
        }

        let this = &mut *slf;
        loop {
            // Drain the buffered batch; skip it entirely if no column matches.
            if let Some(batch) = &this.batch {
                if this.row < batch.len() && this.matched.iter().any(|&m| m) {
                    let row = this.row;
                    this.row += 1;

                    let dict = PyDict::new(py);
                    dict.set_item("stream", batch.stream().stream_id)?;
                    dict.set_item("time", batch.timestamps()[row])?;
                    for (series, matched) in
                        batch.schema().iter().zip(this.matched.iter().copied())
                    {
                        if !matched {
                            continue;
                        }
                        let name = series.metadata().name.as_str();
                        match series.values().get(row) {
                            data::ColumnData::Int(x) => dict.set_item(name, x)?,
                            data::ColumnData::UInt(x) => dict.set_item(name, x)?,
                            data::ColumnData::Float(x) => dict.set_item(name, x)?,
                            _ => dict.set_item(name, "UNKNOWN")?,
                        }
                    }
                    if let Some(ctr) = this.n {
                        this.n = Some(ctr - 1);
                    }
                    return Ok(Some(dict.into()));
                }
            }

            let batch = this.wait_batch(py)?;
            this.matched = column_mask(batch.schema(), &this.columns);
            this.row = 0;
            this.batch = Some(batch);
        }
    }
}

#[pyclass(name = "_Rpc")]
// #[derive(Clone)]
struct PyRpc {
    inner: device::RpcDescriptor,
}

#[pymethods]
impl PyRpc {
    #[getter]
    fn name(&self) -> String {
        self.inner.full_name.clone()
    }

    #[getter]
    fn readable(&self) -> bool {
        self.inner
            .meta
            .flags()
            .contains(proto::RpcMetaFlags::READABLE)
    }

    #[getter]
    fn writable(&self) -> bool {
        self.inner
            .meta
            .flags()
            .contains(proto::RpcMetaFlags::WRITABLE)
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
        self.inner
            .meta
            .flags()
            .contains(proto::RpcMetaFlags::CAPTURE)
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
    inner: device::RpcRegistry,
}

#[pymethods]
impl PyRegistry {
    fn children_of(&self, prefix: &str) -> Vec<String> {
        self.inner.children_of(prefix)
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
    proxy: proxy::Interface,
    route: proto::DeviceRoute,
    rpc: device::RpcClient,
}

#[pymethods]
impl PyDevice {
    #[new]
    #[pyo3(signature = (root_url=None, route=None))]
    fn new(root_url: Option<String>, route: Option<String>) -> PyResult<PyDevice> {
        let root = if let Some(url) = root_url {
            url
        } else {
            "tcp://localhost".to_string()
        };
        let route = if let Some(path) = route {
            path.parse::<proto::DeviceRoute>().map_err(|e| {
                PyValueError::new_err(format!("invalid device route '{}': {}", path, e))
            })?
        } else {
            proto::DeviceRoute::root()
        };
        let proxy = proxy::Interface::new(&root);
        let rpc = device::RpcClient::open(&proxy, route)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to open device: {}", e)))?;
        Ok(PyDevice { root, proxy, route, rpc })
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
        match self.rpc.raw_rpc(&self.route, name, req) {
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
        let readout = device::capture::read_capture(&self.rpc, name, timeout)
            .map_err(|err| {
                PyRuntimeError::new_err(format!("Capture RPC '{}' failed: {}", name, err))
            })?;
        let values = readout
            .values_f64()
            .map_err(|err| {
                PyRuntimeError::new_err(format!("Capture RPC '{}' failed: {}", name, err))
            })?;
        let x_values = readout.x_values_f64();
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
            .rpc
            .registry(&self.route)
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let list: Vec<_> = registry
            .iter()
            .map(|desc| (desc.full_name.clone(), desc.meta.bits()))
            .collect();
        PyList::new(py, list)
    }

    fn _rpc_registry(&self) -> PyResult<PyRegistry> {
        let registry = self
            .rpc
            .registry(&self.route)
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        Ok(PyRegistry { inner: registry })
    }

    #[pyo3(signature = (n=None, stream=None, columns=None))]
    fn _samples(
        &self,
        n: Option<usize>,
        stream: Option<String>,
        columns: Option<Vec<String>>,
    ) -> PyResult<PyIter> {
        let port = self
            .proxy
            .device_full(self.route)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to open device: {}", e)))?;
        Ok(PyIter {
            tree: device::DeviceTree::new(port, proto::DeviceRoute::root()),
            n,
            stream: stream.unwrap_or_default(),
            columns: columns.unwrap_or_default(),
            batch: None,
            row: 0,
            matched: Vec::new(),
        })
    }

    fn _get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let port = self
            .proxy
            .device_full(self.route)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to open device: {}", e)))?;
        let mut device = device::Device::new(port);
        let meta = match device.get_metadata() {
            Ok(meta) => meta,
            Err(_) => return Err(PyRuntimeError::new_err("Failed to get metadata")),
        };

        let dict = PyDict::new(py);

        // Convert device metadata to dict
        let device_dict = PyDict::new(py);
        device_dict.set_item("serial_number", meta.device.serial_number.to_string())?;
        device_dict.set_item("firmware_hash", meta.device.firmware_hash.to_string())?;
        device_dict.set_item("session_id", meta.device.session_id.to_string())?;
        device_dict.set_item("name", meta.device.name.to_string())?;
        dict.set_item("device", device_dict)?;

        // Convert streams to dict
        let streams_dict = PyDict::new(py);
        for (_, stream) in meta.streams {
            let stream_dict = PyDict::new(py);
            stream_dict.set_item("stream_id", stream.stream.stream_id.to_string())?;
            // stream_dict.set_item("name", stream.name.to_string())?;

            let columns_dict = PyDict::new(py);
            for col in stream.columns {
                let col_dict = PyDict::new(py);
                col_dict.set_item("name", col.name.to_string())?;
                col_dict.set_item("description", col.description.to_string())?;
                col_dict.set_item("type", format!("{:?}", col.data_type))?;
                col_dict.set_item("units", col.units.to_string())?;

                columns_dict.set_item(col.name.to_string(), col_dict)?;
            }
            stream_dict.set_item("columns", columns_dict)?;
            streams_dict.set_item(stream.stream.name.to_string(), stream_dict)?;
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
    // Route the twinleaf crate's `log` output to Python's logging module.
    pyo3_log::init();
    m.add_class::<PyDevice>()?;
    m.add_class::<PyRpc>()?;
    m.add_class::<PyRegistry>()?;
    Ok(())
}
