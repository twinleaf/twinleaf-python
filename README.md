# Twinleaf Python (using the Rust backend)

**This library is under active development; interfaces are subject to change.**

**We welcome users to test and provide feedback.**

**These tools only works with devices employing Twinleaf I/O Generation 2, generally devices shipped starting in 2025. For earlier devices please use the tio-python package**

This package implements a communications protocol to work with [Twinleaf sensors](http://www.twinleaf.com) using [Twinleaf I/O (TIO)](https://github.com/twinleaf/libtio/blob/master/doc/TIO%20Protocol%20Overview.md) as the communications layer. Data from the sensors is received via PUB messages and sensor parameters may be changed using REQ/REP messages. This python package uses the twinleaf-rust backend to provide higher performance data handling. 


## Installation

Common platforms support installation from PyPI using:

    `pip install twinleaf`

## Use

Examples of basic usage can be found in the `examples` directory.

A console script `itl` is installed to provide an interactive tool for configuring devices as well.

## Programming

The `twinleaf` module performs metaprogramming to construct an object that has methods that match the RPC calls available on the device. To interact with a Twinleaf CSB current supply, for example:

```python
import twinleaf
csb = twinleaf.Device('serial://COM1')
csb.settings.coil.x.current(0.25) # mA
```

To receive data streams from a sensor such as the [Twinleaf VMR vector magnetometer](http://www.twinleaf.com/vector/VMR), use the named streams:

```python
import twinleaf
vmr = twinleaf.Device('serial://COM1')
print(vmr.samples.vector(10))
```
To find possible tio ports to use run `tio-proxy --enum`. Windows will often output some COMx port such as `serial://COM3`, while Mac OS will output some cu.XXXXX or tty.XXXXX name such as `serial:///dev/cu.usbserialXXXXXX` or `serial:///dev/tty.usbmodemXXXXXX`. 

If `tio-proxy --enum` does not work try looking at serial ports in the respective OS device manager for active serial ports. 


## Migration from `tio-python`

Whereas `tio-python` mixed data stream and rpc methods, this package separates two two under distinct namespaces `samples.*` and `settings.*`. 

Specifying a time duration for the number of samples is not yet supported; only a number of samples can be requested. 


## Prerequisites

[Python](https://www.python.org/downloads/) >= 3.10 is required.


## Windows issues

Windows users who can't run python might need to [add python to their path](https://www.pythoncentral.io/add-python-to-path-python-is-not-recognized-as-an-internal-or-external-command/).

Windows users who run into an error installing packages may need to [enable long paths](https://docs.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation).

Windows console scripts are installed in an odd folder which may be added to your path:

  C:\Users\username\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\LocalCache\local-packages\Python310\Scripts


## Development

Ensure the rust compiler is installed. Use `pip install -e .` to build the package. Publish to PyPI via CI/CD by setting a tag with the version number.
