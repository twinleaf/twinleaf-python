use std::time::Duration;

use ::twinleaf::tio::*;
use ::twinleaf::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

#[pyclass(name = "_DataIterator", subclass)]
struct PyIter {
    port: device::Device,
    n: Option<usize>,
    stream: String,
    columns: Vec<String>,
}

#[pymethods]
impl PyIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let dict = PyDict::new(slf.py());

        // Check if we have a finite count and if it's reached zero
        if let Some(ctr) = slf.n {
            if ctr == 0 {
                // TODO: drop port
                return Ok(None);
            } else {
                slf.n = Some(ctr - 1);
            }
        }
        // If n is None, we continue indefinitely

        while dict.is_empty() {
            // Check for keyboard interrupt
            slf.py().check_signals()?;

            let sample = match slf.port.next() {
                Ok(sample) => sample,
                Err(_) => return Ok(None), // End of stream or error
            };
            if !slf.stream.is_empty() && slf.stream != sample.stream.name {
                continue;
            }

            for sample_column in &sample.columns {
                let sample_column_name = sample_column.desc.name.clone();
                let column_matches = slf.columns.is_empty() || slf.columns.iter().any(|c| {
                    if c.ends_with("*") {
                        // Remove * and check if sample_column_name starts with prefix
                        let prefix = &c[..c.len()-1];
                        sample_column_name.starts_with(prefix)
                    } else {
                        c.eq(&sample_column_name)
                    }
                });
                if column_matches {
                    let time = sample.timestamp_end().into_pyobject(slf.py())?;
                    let stream_id = sample.stream.stream_id.into_pyobject(slf.py())?;
                    dict.set_item("stream", stream_id)?;
                    dict.set_item("time", time)?;
                    match sample_column.value {
                        data::ColumnData::Int(x) => {
                            dict.set_item(sample_column_name.into_pyobject(slf.py())?, x.into_pyobject(slf.py())?)?
                        }
                        data::ColumnData::UInt(x) => {
                            dict.set_item(sample_column_name.into_pyobject(slf.py())?, x.into_pyobject(slf.py())?)?
                        }
                        data::ColumnData::Float(x) => {
                            dict.set_item(sample_column_name.into_pyobject(slf.py())?, x.into_pyobject(slf.py())?)?
                        }
                        _ => dict.set_item(sample_column_name.into_pyobject(slf.py())?, "UNKNOWN".into_pyobject(slf.py())?)?,
                    };
                }
            }
        }

        Ok(Some(dict.into()))
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
        self.inner.readable
    }

    #[getter]
    fn writable(&self) -> bool {
        self.inner.writable
    }

    #[getter]
    fn size_bytes(&self) -> Option<usize> {
        self.inner.size_bytes()
    }

    #[getter]
    fn type_str(&self) -> String {
        self.inner.type_str()
    }

    #[getter]
    fn is_capture(&self) -> bool {
        self.inner.is_capture
    }

    #[getter]
    fn meta_raw(&self) -> u16 {
        self.inner.meta_raw
    }

    fn __repr__(&self) -> String {
        format!(
            "_twinleaf._Rpc({} {}({}))",
            self.inner.perm_str(),
            self.inner.full_name,
            self.inner.type_str(),
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
        self.inner.suggest(query)
    }

    fn search(&self, query: &str) -> Vec<String> {
        self.inner.search(query)
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
            proto::DeviceRoute::from_str(&path).unwrap()
        } else {
            proto::DeviceRoute::root()
        };
        let proxy = proxy::Interface::new(&root);
        let rpc = device::RpcClient::open(&proxy, route.clone()).unwrap();
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
        let port = self
            .proxy
            .subtree_rpc(self.route.clone())
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let specs = device::util::load_rpc_specs(&port)
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let list: Vec<_> = specs
            .into_iter()
            .map(|spec| (spec.full_name, spec.meta_raw))
            .collect();
        Ok(PyList::new(py, list)?)
    }

    fn _rpc_registry(&self) -> PyResult<PyRegistry> {
        let port = self
            .proxy
            .subtree_rpc(self.route.clone())
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let specs = device::util::load_rpc_specs(&port)
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let hash = self
            .rpc
            .get(&self.route, "rpc.hash")
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let mut registry = device::RpcRegistry::new(specs);
        registry.hash = Some(hash);
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
            port: device::Device::new(self.proxy.device_full(self.route.clone()).unwrap()),
            n: n,
            stream: stream.unwrap_or_default(),
            columns: columns.unwrap_or_default(),
        })
    }

    fn _get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let mut device = device::Device::new(self.proxy.device_full(self.route.clone()).unwrap());
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
    m.add_class::<PyDevice>()?;
    m.add_class::<PyRpc>()?;
    m.add_class::<PyRegistry>()?;
    Ok(())
}
