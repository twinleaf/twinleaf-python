# Proposal: multiple sensors and synchronized data in the Python API

Status: phases 1 (the tree), 2 (synchronized acquisition) and 3 (unified
settings) are implemented; phase 4 is a proposal, for review. Written 9 November 2026 against twinleaf 2.0.0 and the VMR-SANDI setup
connected on `/dev/cu.usbmodem14201`.

Phase 1 as built:

- **`twinleaf.connect(url=None, *, discover=2.0)` and `twinleaf.Tree`**, as
  sketched below: `tree[route]`, iteration in route order, `len`, `in`,
  `find(name=, serial=)`, `refresh(seconds)`, and `info` / `routes` for what
  discovery found without opening any device.
- **One connection per URL.** Trees are cached per URL, held weakly.
  `twinleaf.Device(url, route)` goes through the same cache and keeps its tree
  alive as `device.tree`. Opening several devices on one port uses one socket
  to the proxy, where it used one each before.
- **`Device()` with no URL** now uses `Connection::connect`, the same as
  `connect()`. It starts or hosts a proxy when none runs, where it used to
  need one already running at `tcp://localhost`.
- **Serials:** discovery reads `dev.serial`. When that is blank, as on this
  VMR, it uses the device serial from the metadata instead.
- **Unknown routes:** `tree[route]` raises `KeyError` for a route discovery
  did not find, instead of waiting for RPC timeouts.

Phase 2 as built, with how it differs from section 3 below:

- **API:** `tree.acquire(streams=None, *, retention=60)` returns an
  `Acquisition` with `wait()`, `window()`, `slice()` and `close()`, which
  also works as a context manager. `tree.samples(seconds, streams)` does all
  of it in one call. It returns a `Slice` of `StreamSlice`s, each with numpy
  `time` and column arrays, `units`, `rate_hz`, `timebase` and `gaps`.
- **Buffering:** a Rust thread drains `tree.samples()` into one ring per
  stream, without the GIL. Each ring holds the rows' times and their
  column values, in each column's native numpy dtype. Python receives
  bytes and wraps them with `np.frombuffer`, so the extension does not
  depend on the `numpy` crate. numpy is now a required Python dependency.
- **Known streams:** the streams of every device are learned from metadata
  when the acquisition starts. `wait()` therefore waits for slow streams too,
  instead of only the streams that happened to arrive first.
- **`wait(seconds)`** waits until `window(seconds)` would succeed, not for a
  fixed time. It raises `NotEnoughData` after a timeout, naming the stream
  that is short, such as one that is disabled.
- **Selection rules:**
  - Rows are those whose end-of-sample time lies in `(start, end]`, so
    adjacent slices share no row.
  - A stream covers an interval when its first retained sample began by
    `start`, and its next sample would end after `end`.
  - `slice()` requires every stream to cover the interval, unless
    `trim="overlap"`.
- **Mixed timebases:** `allow_mixed_timebases` exists only on `window()`. An
  explicit `slice(start, end)` cannot apply one interval to two clocks.
- **Wall-clock offset:** `host_time_offset` is the *minimum* host-minus-device
  difference seen, not the median. Transport delay only ever adds to the
  difference, so the minimum is the closest estimate. It is exactly 0 for a
  UNIX epoch.
- **Run breaks:** a device restart, a timebase change, time going backward, or
  a change of columns clears the stream's rows. A window that would reach
  before the break raises `NotEnoughData` with the reason.
- **Not yet done:** an acquisition does not issue `DeviceLostWarning`; a lost
  device shows as `NotEnoughData` from `wait()`, and a lost proxy as
  `ConnectionError`.
- **Tested** on the simulator: streams of one device at 1000 and 25 Hz, two
  devices with different clocks, a device restart, a proxy stop, and lost
  samples.
- **Tested on the VMR-SANDI** (10 November 2026), on the hub-clock case the
  simulator cannot produce:
  - The VMR's `/0/vector` and the COMM-USB's `/imu`, `/bar`, `/aux` and
    `/sync` were accepted as one timebase, `systime` of `COMM-USB.00084`.
  - A 5 s slice held exactly 1000 / 50 / 20 rows of vector / imu / bar, all
    inside the interval.
  - A 30 s window of all five streams held exactly 6000 / 300 / 120 / 300 / 30
    rows on uniform steps, with no gaps and no dropped batches.

Phase 3 as built, as sketched in section 2 below, plus:

- **Choosing devices:** `tree.unify(name)` takes every device whose `dev.name`
  matches, ignoring case. `tree.unify([routes or Devices])` takes the devices
  listed.
- **Shared RPCs:** the unified settings are the RPCs every member has with an
  identical `rpc.info` word, so the same type and access. `not_shared` lists
  the others; for the COMM-USB with the VMR, that is 62 of them.
- **Calls:** each unified RPC calls the members' own RPC objects with the
  same arguments, so argument checking is exactly as for one device.
  - Reads and writes return the common value, or `twinleaf.Mixed`.
  - Captures return one capture per route.
  - Actions return None.
  - Surveys combine per RPC.
- **Failures:** a failure on some members raises `twinleaf.UnifiedRpcError`, a
  `RuntimeError`, after every member was tried, with `results` and `errors`
  by route. The same failure on every member, such as an argument of the
  wrong type, raises that error itself.
- **Samples:** `Unified.samples(seconds, ["vector"])` reads the named streams
  of every member through `Tree.samples`. `stream_paths()` gives their paths.
  Devices behind different hubs need `allow_mixed_timebases=True`.
- **Tested:**
  - On a three-device simulated tree: writes, agreeing and mixed reads,
    surveys, captures, partial failures and argument errors.
  - Read-only on the VMR-SANDI: the VMR alone, and the COMM-USB with the VMR,
    whose shared settings are `dev.*` and `rpc.*` only.

## What we have to work with

### The connected setup

One serial port carries a two-device tree:

| Route | Device | Streams (rate) |
|---|---|---|
| `/` | COMM-USB R8, `COMM-USB.00084` | `imu` accel + gyro (10 Hz), `bar` pressure + temperature (4 Hz), `aux` vbus + MCU temperature (10 Hz), `sync` VCO error + pull (1 Hz) |
| `/0` | VMR R3 | `vector` x, y, z in nT (200 Hz) |

Every stream on both devices reports the same timebase in its segment metadata:
`epoch = SYSTIME`, `timeref_serial = "COMM-USB.00084"`, `timeref_session =
240050454`. The VMR keeps its own device session, but its sample clock is
disciplined by the COMM-USB. Its `start_time` is 16 s against the hub's 6 s,
and both lie on the hub's time axis. So timestamps from the two devices can be
compared directly. They are seconds of the COMM-USB's uptime, not UNIX time.

Both devices' timestamps track the host clock with the same offset, to within
batching. The slow streams (`bar`, `sync`) reach the host about 1 s later than
`vector`, because their samples are batched.

### What the Python API does today

- **One device per object.** Each `twinleaf.Device(url, route)` opens its own
  connection and binds one route. Reaching the VMR means knowing to pass
  `route="/0"`. Nothing lists the tree.
- **`samples(n)` blocks per call.** Each call opens a fresh subscription and
  waits for `n` rows of that one request. Two calls on two devices return
  different time windows that do not overlap, so there is no way to get data
  from both devices covering the same interval.
- **Rows are timestamped but not related.** Each row's `time` is the end of the
  sample interval in device seconds, which is the right quantity. But no
  timebase is reported, so nothing tells the user whether two `time` columns
  can be compared.

### What twinleaf 2.0 provides

- **One tree, several routes:** `Connection::open(url)` reaches the whole tree,
  and `connection.tree(route).samples()` gives one receiver for every route,
  each batch tagged with its route. `open_layout(Layout { mounts })` combines
  several URLs into one tree at `/0`, `/1`, …, which is how the app opens
  several sensors.
- **Discovery:** `DeviceTree::named_routes(window)` lists routes with their
  `dev.name`, and `routes_updates()` follows changes.
- **Timebase in the metadata:** each batch's segment gives `epoch`,
  `timeref_serial`, `timeref_session`, `start_time`, `sampling_rate` and
  `decimation`. Boundaries name `TimeRefSessionChanged`, `SessionChanged`,
  `Restarted`, `TimeBackward`, `RateChanged` and data `Loss`.
- **No alignment.** Nothing merges, aligns, resamples or interpolates streams.
  The HDF5 export says it plainly: "Align rows on the `time` field, not on row
  index." `data::Buffer` keeps per-stream history, but has no public
  time-window query.

### How the Twinleaf apps do it

- **Unify Matching Sensors** (macOS and GTK) groups devices with the same
  `dev.name` in the sidebar. A column caption then plots that column from every
  member, and a setting row writes to every writable member and shows "mixed"
  when their values differ. Devices are told apart by `route · serial`. It
  changes grouping only, not the data path.
- **There is no "synchronized" mode.** Alignment is implicit: one subscription
  to the whole tree, a per-column deque of `(timestamp, value)`, and every plot
  takes `[end − window, end]` from each column by binary search. `end` is the
  latest timestamp of the plot's first column. There is no host time, no
  resampling, and no trimming to the overlap.
- **The timebase is never compared.** A column whose time does not land in the
  window is flagged "time reference doesn't match" after the fact.
- **The planned `align` node** (`twinleaf-app/docs/signal-graph.md`, not
  implemented) states the rules we should adopt:
  - require the same epoch and timeref session;
  - refuse to resample implicitly and require matching rates;
  - correct sub-sample offset once;
  - emit over the intersection of the inputs.

## Proposal

The proposal has two layers: a tree object for reaching devices and their
settings, and an acquisition object for synchronized data. The existing
`Device` API stays as it is.

### 1. The tree: every device and its settings

```python
import twinleaf

tree = twinleaf.connect("serial:///dev/cu.usbmodem14201")
print(tree)
# twinleaf.Tree('serial:///dev/cu.usbmodem14201')
#   /   COMM-USB  COMM-USB.00084
#   /0  VMR       01000000170030001450415452373720

hub = tree["/"]                 # a twinleaf.Device, as today
vmr = tree["/0"]
vmr.settings.vector.data.decimation()          # unchanged settings API
hub.settings.sync.data.enable()
vmr.samples.vector(100)                         # unchanged per-device samples

tree.find(name="VMR")           # -> [Device], matched on dev.name / serial / route
for dev in tree:                # every device, in route order
    ...
```

- **`twinleaf.connect(url=None)`** opens one connection. `None` means the
  default device, through `Connection::connect`, which also starts a proxy if
  none is running. A list of URLs opens a layout mounted at `/0`, `/1`, …, as
  the app does.
- **Devices share the connection.** `tree[route]` returns a `Device` bound to
  that route on the tree's connection, so N devices cost one link, not N.
  `twinleaf.Device(url, route)` keeps working, and should reuse a per-process
  connection for the same URL.
- **Indexing is by route.** Routes are stable and unique, while names are not:
  two VMRs both answer `VMR`. `find()` matches on name, serial or route. I
  propose not to synthesize attributes such as `tree.vmr`, because they would
  break as soon as a second unit is plugged in.
- **Devices that come and go:** `tree.refresh()` re-runs discovery. A device
  that drops out keeps its `Device` object; its calls fail with the existing
  reasons and `DeviceLostWarning`.

### 2. Unified settings: the app's "unify", in Python

```python
vmrs = tree.unify("VMR")                   # every device whose dev.name is VMR
vmrs.settings.vector.data.decimation(8)    # writes to every writable member
vmrs.settings.vector.data.decimation()     # 8 if all agree, else Mixed({'/0': 8, '/1': 4})
vmrs.samples.vector(...)                   # see section 3: one synchronized slice
```

- The RPC tree is the intersection of the members' RPC tables, matching the
  app's merged rows.
- A write goes to every member. If any member fails, it raises after trying
  all, and the error lists which routes failed.
- A read returns the value when every member agrees. Otherwise it returns a
  `twinleaf.Mixed` mapping of route → value, the app's "mixed" state as a
  value the code can inspect rather than a guess.

### 3. Synchronized data: one acquisition, many streams

The central piece is one subscription to the whole tree. A background thread
drains it with the GIL released, into per-stream ring buffers of timestamps and
column values. All slices are cut from those buffers, so every stream in a slice
comes from the same interval by construction.

```python
with tree.acquire(retention=60) as acq:        # starts buffering every stream now
    acq.wait(10)                                 # or do other work

    s = acq.window(5)                            # the latest 5 s that every stream has
    s = acq.slice(5120.0, 5125.0)                # an explicit device-time interval
    s = acq.window(5, streams=["/0/vector", "/imu"])

s["/0/vector"].time         # numpy float64, end-of-sample device seconds
s["/0/vector"]["x"]         # numpy array, nT
s["/imu"]["accel.z"]
s.timebase                  # Timebase(epoch='systime', serial='COMM-USB.00084', session=240050454)
s.start, s.end              # the slice's interval
s.gaps                      # [(stream, t_from, t_to, samples_lost)]
s.to_pandas()               # one DataFrame per stream, or a long-format frame
```

For one-off reads, `tree.samples(seconds=5, streams=[...])` does
`acquire` + `wait` + `window` in one call. That is the synchronized counterpart
to today's `samples(n)`.

#### Rules, following the planned `align` node

- **Device time only.** A slice is cut by device timestamps, never by host
  receive time. Rows are selected by end-of-sample time, the same `time` the
  per-device API reports today.
- **Check the timebase first.** Every stream in a slice must share
  `(epoch, timeref_serial, timeref_session)`. If they don't, `window()` raises
  `twinleaf.TimebaseMismatch`, naming the groups it found. This turns the app's
  after-the-fact "time reference doesn't match" into an error before any data
  is returned. `allow_mixed_timebases=True` opts out and returns each group
  with its own timebase.
- **A window ends where every stream is complete.** `window(seconds)` ends at
  the *minimum* over the selected streams of their latest timestamp, so slow,
  batched streams such as `bar` and `sync` are complete up to the right edge.
  The app instead anchors to one column's latest point, which here would leave
  the last second of `bar` empty.
- **Jagged by default.** Each stream keeps its own rate and its own `time`
  array: 200 Hz vector, 10 Hz imu, 4 Hz bar. Nothing is resampled implicitly.
- **Resampling is explicit:**
  `s.resample(on="/0/vector", method="linear" | "previous")` returns one table
  on the reference stream's time grid. It is only allowed within one timebase,
  and every interpolated column keeps its source rate in its metadata.
  `method="previous"` (zero-order hold) suits slow housekeeping data such as
  `bar`; `"linear"` suits signals sampled well above their bandwidth.
- **Trimming is a choice.** `trim="overlap"` cuts every stream to the interval
  all of them cover. `window()` covers this already, but an explicit `slice()`
  may not.
- **Boundaries cut runs.** `SessionChanged`, `Restarted`,
  `TimeRefSessionChanged` and `TimeBackward` end a stream's run. A slice never
  spans a break; asking for one raises with the time of the break. Data `Loss`
  stays inside a run, is listed in `s.gaps`, and still issues
  `SampleLossWarning`.
- **Wall-clock time is opt-in.** `epoch = SYSTIME` has no absolute meaning.
  `s.host_time_offset` gives the median host-minus-device offset seen during the
  acquisition, so `s.time + s.host_time_offset` approximates UNIX time to about
  the transport latency. It is labelled as an estimate. When the epoch is UNIX,
  the offset is 0 and exact.

#### On the VMR-SANDI

```python
tree = twinleaf.connect("serial:///dev/cu.usbmodem14201")
s = tree.samples(seconds=10, streams=["/0/vector", "/imu", "/bar"])
# 2000 vector rows, 100 imu rows, 40 bar rows, all within the same 10 s of
# COMM-USB time, checked to share the COMM-USB timebase
aligned = s.resample(on="/0/vector", method="previous")
# one 2000-row table: vector.x/y/z, imu.*, bar.* held at their last value
```

## Implementation sketch

In the Rust extension:

1. **`_Tree`** wraps `Connection` + `DeviceTree`, with `named_routes`,
   `device(route)` and `open_layout` for URL lists. `_Device` gains a
   constructor from a tree, so devices share its connection.
2. **`_Acquisition`** owns a thread that drains `tree.samples()` into
   `HashMap<StreamKey, Ring>`. A ring holds timestamps, sample numbers, one
   `Vec` per column, the run's timebase, and its gaps. It is pruned by
   retention seconds and a row cap, like the app's deque. It is not built on
   `data::Buffer`, which cannot be sliced by time. It takes the existing loss
   and presence warnings over from the per-device iterator.
3. **Slices are numpy arrays.** This adds `numpy` as a dependency and the
   `numpy` crate on the Rust side; its support for pyo3 0.30 needs checking
   first. Resampling can live in Python on top of numpy (`np.interp`,
   `np.searchsorted`).
4. **Unified settings and `find()`** are pure Python over `_Tree`.

Phases, each shippable on its own:

1. Tree, shared connection and discovery.
2. Acquisition, `window`, `slice`, timebase check.
3. Unified settings.
4. Resampling and pandas export.

## Open questions

- **Device naming:** is route indexing plus `find()` enough, or do users want
  generated attributes despite the collision problem?
- **Unified reads:** should a mixed read return `Mixed` or raise?
- **Window end:** is "latest time every stream has" the right default, or should
  it follow the app and anchor to one stream?
- **numpy:** is a required dependency acceptable? The package already requires
  IPython.
- **Default retention:** what should it be? The app keeps between the plot
  window + 5 s and 1500 s.
