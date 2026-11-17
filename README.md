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
#   /0  MAG       01000000170030001450415452373720

mag = tree['/0']                   # a twinleaf.Device, with settings.* and samples.*
hub = tree['/']
mag.settings.vector.data.decimation()
hub.samples.imu(10)
tree.find(name='MAG')              # devices by dev.name, or serial=...
for route, device in tree.items():
    print(route, device)
```

The devices of a tree share one connection. `twinleaf.Device(url, route)` also reuses the tree for that `url`, so opening several devices on one port costs one connection, and it returns the same object as `tree[route]`. `twinleaf.connect([url1, url2])` mounts several devices as one tree at `/0`, `/1`, …. Discovery listens for two seconds; `tree.refresh()` looks again.

`tree.close()` ends the connection, as does leaving `with twinleaf.connect(url) as tree:`. Every device on the connection stops working, and the port is released a few seconds later.

### Several devices of one kind

`tree.unify(name)` addresses every device with that `dev.name` as one, like the Twinleaf app's "Unify Matching Sensors":

```python
mags = tree.unify('MAG')                    # or tree.unify(['/0', '/1'])
mags.settings.vector.data.decimation(8)     # writes to every magnetometer
mags.settings.vector.data.decimation()      # 8 if every magnetometer agrees, else
                                            # twinleaf.Mixed({'/0': 8, '/1': 4})
mags.settings.vector.data()                 # a survey, with Mixed values where they differ
mags.samples('vector', seconds=5)           # the same 5 s of every magnetometer's vector stream
```

The unified settings are the RPCs that every device has with the same type; `mags.not_shared` lists the rest. A call is tried on every device. If it fails on any of them, `twinleaf.UnifiedRpcError` is raised, holding the replies and errors by route. The devices are called one after another, and a write that fails on one stays written on the others. In `mags.samples()`, a stream name such as `vector` means that stream of every device. A `Mixed` value has no truth value, so `if mags.settings.x():` raises when the devices disagree instead of passing.

### Synchronized data from several devices

Samples read from two devices with separate `samples(n)` calls cover different time intervals. To get the same interval from several streams, read them from the tree:

```python
s = tree.samples(['/0/vector', '/imu', '/bar'], seconds=5)    # the same 5 s of every stream
s['/0/vector'].time        # numpy array of end-of-sample times, in device seconds
s['/0/vector']['x']        # numpy array of vector.x, in nT
s['/imu']['accel.z']
s.timebase                 # the clock every stream counts on
s.gaps                     # samples lost within the slice
```

The length is given in seconds and by name, as the streams have different rates; `device.samples.vector(10)` counts samples instead. A slice is a mapping of stream path to stream, so `s.items()` and `s.values()` work as on a dict. A stream is named by its device's route and its name: `/imu` on the device at `/`, `/0/vector` on the device at `/0`. A name with no slash, such as `vector`, is the one stream with that name, and raises when several devices have it. Each stream keeps its own rate and its own `time` array; nothing is resampled. With no streams named, every stream that is enabled on its device is read.

For repeated reads, an acquisition buffers every stream in the background:

```python
with tree.acquire(retention=60) as acq:    # keeps the latest 60 s of every stream
    acq.wait(10)                            # until 10 s of every stream are buffered
    s = acq.window(10)                      # the latest 10 s that every stream has
    s = acq.slice(5120.0, 5125.0)           # an explicit interval of device time
```

- The interval is in device time, never the time the host received the data. A window ends at the newest time every stream has reached, so slow streams such as `bar` at 4 Hz are complete up to its end. A slice holds the rows whose end-of-sample time lies in `(start, end]`.
- Streams can only be compared if they count time on the same clock. Sensors behind one hub share the hub's clock. If the requested streams don't share one, `twinleaf.TimebaseMismatch` is raised. `allow_mixed_timebases=True` returns each clock's streams over that clock's latest window instead, and `s.by_timebase()` splits such a slice by clock.
- `twinleaf.NotEnoughData` says which stream is short when the buffer doesn't cover the interval. That happens just after starting, or after a device restart, which ends a stream's run.
- Lost samples are listed in `s.gaps` and reported once each with `twinleaf.SampleLossWarning`.
- A `ConnectionError` is raised by `wait()` and `window()` if the proxy goes away. If one stream stops, the latest window stops moving, as it ends where that stream ends; `s.age` then keeps growing. It is the seconds since the newest row arrived of the stream heard from least recently.
- Device time is usually the uptime of the device that owns the clock. `s.host_time_offset` estimates the seconds to add for UNIX time.

To put every stream on one time grid, resample onto one stream's times. There is no default method, because the right one depends on the signal:

```python
r = s.resample(on='/0/vector', method='previous')     # hold the latest earlier row
r = s.resample(on='/0/vector', method={'/imu': 'linear', '/bar': 'previous'})
r['/bar']['measure']       # now one value per vector row
```

- `previous` holds the latest row that ended at or before each time. It suits slow housekeeping data.
- `linear` interpolates between neighboring rows. It suits signals sampled well above their bandwidth.
- Streams are aligned by end-of-sample time; device filter delays are not corrected.
- A time where a stream has no value is NaN. That is before the stream's first row, after its last row for `linear`, or inside one of its gaps. `edges='trim'` drops those times instead.
- `linear` gives float64. `previous` keeps a float column's dtype and makes any other column float64, so that it can hold NaN; with `edges='trim'` it keeps every dtype.
- A method applies to every column of its stream, so use `previous` for a stream with counters or status flags. Resampling a fast stream onto a slower one does not average, so decimate it on the device first.
- The arrays of a slice are read-only; copy one to change it.

See [docs/multi-sensor-api.md](docs/multi-sensor-api.md) for the design.

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
