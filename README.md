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

To find possible tio ports to use run `tio list`. Windows will often output some COMx port such as `serial://COM3`, while Mac OS will output some cu.XXXXX or tty.XXXXX name such as `serial:///dev/cu.usbserialXXXXXX` or `serial:///dev/tty.usbmodemXXXXXX`. 

If `tio list` does not find your device, try `tio list --all`, or look at serial ports in the respective OS device manager for active serial ports. 

### Several devices

A hub and the sensors behind it share one port, and each device has a route: `/` for the device on the port, `/0`, `/1`, … for the devices behind it. `twinleaf.connect()` finds every device and returns them as a tree:

```python
import twinleaf
tree = twinleaf.connect('serial:///dev/cu.usbmodem14201')
print(tree)
# twinleaf.Tree('serial:///dev/cu.usbmodem14201')
#   /   COMM-USB  COMM-USB.00084
#   /0  VMR       01000000170030001450415452373720

vmr = tree['/0']                   # a twinleaf.Device, with settings.* and samples.*
hub = tree['/']
vmr.settings.vector.data.decimation()
hub.samples.imu(10)
tree.find(name='VMR')              # devices by dev.name, or serial=...
for device in tree:
    print(device)
```

The devices of a tree share one connection. `twinleaf.Device(url, route)` also reuses the tree for that `url`, so opening several devices on one port costs one connection. `twinleaf.connect([url1, url2])` mounts several devices as one tree at `/0`, `/1`, …. Discovery listens for two seconds; `tree.refresh()` looks again.

Samples read from two devices with separate `samples(n)` calls cover different time intervals. Reading synchronized slices across devices is planned; see [docs/multi-sensor-api.md](docs/multi-sensor-api.md).

### The tio proxy

Every connection goes through a proxy on `localhost:7855`, which shares each device between processes. `twinleaf.Device()` with no URL uses the default device: the one a running proxy selects, or else the single Twinleaf serial device attached. When no proxy is running, opening a device starts `tio proxy` in the background. If `tio` is not installed, the Python process hosts the proxy itself, and other scripts connected through it lose their devices when that process exits. Installing the tools is recommended:

    cargo install twinleaf-tools

If the proxy stops, reading samples raises `ConnectionError`. If the proxy's process ends abruptly, such as a script that hosted it exiting, the connection spends up to 30 s trying to reconnect before it raises. While samples are being read, `twinleaf.DeviceLostWarning` reports a device that stops responding, and `twinleaf.SampleLossWarning` reports samples lost because the reader fell behind or the device or its link dropped them. Both are Python warnings, so they can be silenced or raised as errors:

```python
import warnings, twinleaf
warnings.simplefilter("error", twinleaf.SampleLossWarning)
```

This package requires `twinleaf-tools` 2.0 or later. It cannot connect to a proxy from an older version, such as the 1.x `tio-proxy` command. Stop any older proxy before connecting.


## Migration from `tio-python`

Whereas `tio-python` mixed data stream and rpc methods, this package separates two two under distinct namespaces `samples.*` and `settings.*`. 

Specifying a time duration for the number of samples is not yet supported; only a number of samples can be requested. 


## Prerequisites

[Python](https://www.python.org/downloads/) >= 3.12 is required.


## Windows issues

Windows users who can't run python might need to [add python to their path](https://www.pythoncentral.io/add-python-to-path-python-is-not-recognized-as-an-internal-or-external-command/).

Windows users who run into an error installing packages may need to [enable long paths](https://docs.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation).

Windows console scripts are installed in an odd folder which may be added to your path:

  C:\Users\username\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\LocalCache\local-packages\Python310\Scripts


## Development

Ensure the rust compiler is installed. Use `pip install -e .` to build the package. Publish to PyPI by incrementing the version number on a clean build.
