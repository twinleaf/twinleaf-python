import weakref
from collections.abc import Iterator, Sequence
from typing import Any, NamedTuple

from twinleaf import _twinleaf
from twinleaf._acquisition import Acquisition as Acquisition
from twinleaf._acquisition import Gap as Gap
from twinleaf._acquisition import NotEnoughData as NotEnoughData
from twinleaf._acquisition import Slice as Slice
from twinleaf._acquisition import StreamSlice as StreamSlice
from twinleaf._acquisition import Timebase as Timebase
from twinleaf._acquisition import TimebaseMismatch as TimebaseMismatch
from twinleaf._twinleaf import DeviceLostWarning as DeviceLostWarning
from twinleaf._twinleaf import SampleLossWarning as SampleLossWarning


class Device(_twinleaf._Device):
    """Primary TIO interface with sensor object.

    `url` names the device, or the default device when it is None. Devices
    opened with the same `url` share one connection, the `Tree` that
    `twinleaf.connect(url)` returns, which is available as `device.tree`.
    """

    tree: "Tree"

    def __new__(
        cls, url=None, route=None, announce=False, instantiate=True, *, tree=None
    ):
        if tree is None:
            tree = connect(url, discover=0)
        device = super().__new__(cls, None, route, tree._tree)
        # Holding the tree keeps the shared connection, and its cache entry,
        # alive as long as the device.
        device.tree = tree
        return device

    def __init__(
        self, url=None, route=None, announce=False, instantiate=True, *, tree=None
    ):
        super().__init__()
        if instantiate:
            self._instantiate_rpcs()
            self._instantiate_samples(announce)

    def __repr__(self):
        try:
            dev_info = self._rpc("dev.serial", b"").decode()
        except RuntimeError:
            dev_info = ""
        return f"{self.__module__}.{self.__class__.__name__}('{dev_info}', url='{self._url}', route='{self._route}')"

    def _rpc_int(
        self, name: str, size: int, signed: bool, value: int | None = None
    ) -> int:
        """Use struct to send int-typed RPCs"""
        import struct

        match size, signed:
            case 1, True:
                fstr = "<b"
            case 2, True:
                fstr = "<h"
            case 4, True:
                fstr = "<i"
            case 8, True:
                fstr = "<q"
            case 1, False:
                fstr = "<B"
            case 2, False:
                fstr = "<H"
            case 4, False:
                fstr = "<I"
            case 8, False:
                fstr = "<Q"
            case _:
                raise ValueError(f"Unsupported int size/signed combo: {size}, {signed}")
        payload = b"" if value is None else struct.pack(fstr, value)
        rep = self._rpc(name, payload)
        val = struct.unpack(fstr, rep)[0]
        return val

    def _rpc_float(self, name: str, size: int, value: float | None = None) -> float:
        """Use struct to send float-typed RPCs"""
        import struct

        fstr = "<f" if (size == 4) else "<d"
        payload = b"" if value is None else struct.pack(fstr, value)
        rep = self._rpc(name, payload)
        val = struct.unpack(fstr, rep)[0]
        return val

    def _instantiate_rpcs(self):
        """Set up Device.samples, then recursively instantiate RPCs"""
        self._registry = self._rpc_registry()
        self.settings = _RpcSurvey("settings")
        self._instantiate_rpcs_recursive(self.settings)

    def _instantiate_rpcs_recursive(self, parent, prefix=""):
        """Get children from registry, setattr them, then recurse"""
        for child_name in self._registry.children_of(prefix):
            full_path = f"{prefix}.{child_name}" if prefix else child_name
            rpc = self._registry.find(full_path)
            attr_name = "_rpc" if child_name == "rpc" else child_name

            if rpc is not None:
                child = _Rpc(rpc, self)
            else:
                child = _RpcSurvey(attr_name)
            setattr(parent, attr_name, child)
            self._instantiate_rpcs_recursive(child, full_path)

    def _samples_dict(
        self, n: int = 1, stream: str = "", columns: list[str] | None = None
    ) -> dict[int, dict[str, list[int | float]]]:
        """Parse underlying sample iterator into dict"""
        if columns is None:
            columns = []  # Avoid mutable default
        samples = list(self._samples(n, stream=stream, columns=columns))
        # Bin into streams
        streams = {}
        for line in samples:
            stream_id = line.pop("stream", None)
            if stream_id not in streams:
                streams[stream_id] = {"stream": stream_id}
            for key, value in line.items():
                if key not in streams[stream_id]:
                    streams[stream_id][key] = []
                streams[stream_id][key].append(value)
        return streams

    def _samples_list(
        self,
        n: int = 1,
        stream: str = "",
        columns: list[str] | None = None,
        time_column=True,
        title_row=True,
    ) -> list[list[str | int | float]]:
        """Parse underlying sample iterator into tabular array"""
        if columns is None:
            columns = []  # Avoid mutable default
        streams = self._samples_dict(n, stream, columns)
        # Convert to list with rows of data. Not super happy about how inefficient this is.
        if len(streams.items()) > 1:
            raise NotImplementedError(
                "Stream concatenation not yet implemented for two different streams"
            )
        stream_dict = list(streams.values())[0]
        stream_dict.pop("stream")
        if not time_column:
            stream_dict.pop("time")
        data_columns = [column for column in stream_dict.values()]
        data_rows = [list(row) for row in zip(*data_columns)]
        if title_row:
            column_names = list(stream_dict.keys())
            data_rows.insert(0, column_names)
        return data_rows

    def _instantiate_samples(self, announce: bool = False):
        metadata = self._get_metadata()
        if announce:
            dev_meta = metadata["device"]
            print(
                f"{dev_meta['name']} ({dev_meta['serial_number']}) [{dev_meta['firmware_hash']}]"
            )

        streams_flattened = []
        for stream, value in metadata["streams"].items():
            for column_name in value["columns"].keys():
                streams_flattened.append(stream + "." + column_name)

        # All samples
        self.samples = _SamplesDict(self, "samples", stream="", columns=[])

        for stream_column in streams_flattened:
            stream, *prefix, mname = stream_column.split(".")
            parent = self.samples

            # All samples for this stream
            if not hasattr(parent, stream):
                setattr(
                    parent,
                    stream,
                    _SamplesList(self, name=stream, stream=stream, columns=[]),
                )
            parent = getattr(parent, stream)

            # Wildcard columns within stream
            stream_prefix = ""
            for token in prefix:
                stream_prefix += "." + token
                if not hasattr(parent, token):
                    setattr(
                        parent,
                        token,
                        _SamplesList(
                            self, token, stream, columns=[stream_prefix[1:] + ".*"]
                        ),
                    )
                parent = getattr(parent, token)

            # Specific stream samples
            stream, column_name = stream_column.split(".", 1)
            setattr(
                parent, mname, _SamplesList(self, mname, stream, columns=[column_name])
            )

    def _interact(self):
        imported_objects = {}
        imported_objects["tl"] = self
        try:
            import IPython

            IPython.embed(
                user_ns=imported_objects,
                banner1="",
                banner2="",  # Use   : {self._shortname}.<tab>
                exit_msg="",
                enable_tip=False,
            )
        except ImportError:
            import code

            repl = code.InteractiveConsole(locals=imported_objects)
            repl.interact(banner="", exitmsg="")


type _rpc_type = int | float | str | bytes | None


def _is_capture_rpc(pyrpc: _twinleaf._Rpc) -> bool:
    meta_raw = getattr(pyrpc, "meta_raw", 0)
    return (
        getattr(pyrpc, "is_capture", False)
        or pyrpc.type_str == "capture"
        or (meta_raw & 0x1000) != 0
        or pyrpc.name.rsplit(".", 1)[-1] == "capture"
    )


class _RpcNode:
    """Base class for RPCs and surveys in the device tree"""

    def __init__(self, name: str):
        self.__name__ = name

    def __repr__(self):
        return f"{self.__module__}.{self.__class__.__name__}('{self.__name__}')"

    def _survey(self) -> dict[str, _rpc_type]:
        """Recursively collect all readable RPC values in this subtree"""
        results = {}
        for name, attr in self.__dict__.items():
            if isinstance(attr, _RpcNode):
                # Check if it's an RPC that should be read
                if isinstance(attr, _Rpc):
                    if attr._readable and attr._type not in {None, bytes}:
                        results[attr.__name__] = attr._call()

                # Recursively survey children (works for both Rpc and Survey)
                results |= attr._survey()
        return results


class _RpcSurvey(_RpcNode):
    """ " Branch object that can collect all callable child RPC values"""

    def __init__(self, name: str):
        super().__init__(name)

    def __call__(self) -> dict[str, _rpc_type]:
        return self._survey()


class _Rpc(_RpcNode):
    """Base class for RPCs"""

    def __new__(cls, pyrpc: _twinleaf._Rpc, device: Device):
        match pyrpc:
            case r if _is_capture_rpc(r):
                subclass = _RpcCapture
            case r if r.type_str == "" and r.size_bytes != 0:
                subclass = _RpcReadWrite  # unknown/bytes rpc
            case r if r.readable and r.writable:
                subclass = _RpcReadWrite
            case r if r.writable:
                subclass = _RpcWriteOnly
            case r if r.readable:
                subclass = _RpcReadOnly
            case _:
                subclass = _RpcAction
        rpc = super().__new__(subclass)  # type: ignore[arg-type]
        return rpc

    def __init__(self, pyrpc: _twinleaf._Rpc, device: Device):
        super().__init__(pyrpc.name)
        self._device = device
        self._meta_raw = getattr(pyrpc, "meta_raw", 0)
        self._data_size = pyrpc.size_bytes
        self._readable = pyrpc.readable
        self._writable = pyrpc.writable
        self._type: type | None = None
        self._is_capture = _is_capture_rpc(pyrpc)
        match pyrpc.type_str:
            case "capture":
                self._data_type = None
            case "bool":
                self._type = bool
                self._data_type = 0
            case t if t.startswith("u"):
                self._type = int
                self._data_type = 0
                self._signed = False
            case t if t.startswith("i"):
                self._type = int
                self._data_type = 1
                self._signed = True
            case t if t.startswith("f"):
                self._type = float
                self._data_type = 2
            case t if t.startswith("s"):
                self._type = str
                self._data_type = 3
            case "" if self._data_size == 0:
                self._type = None
                self._data_type = 0
            case _:
                self._type = bytes
                self._data_type = 0

    def __repr__(self):
        if self._is_capture:
            return f"{self.__module__}.{self.__class__.__name__}('{self.__name__}', capture=True)"
        ret = super().__repr__().strip(")") + ", "
        if hasattr(self, "_signed") and not self._signed:
            ret += "u"
        ret += self._type.__name__ if self._type is not None else "none"
        if self._data_size and self._type is not bool:  # is not 0 or None
            ret += str(self._data_size * 8)
        ret += ")"
        return ret

    def _call(self, arg: _rpc_type = None, *, raw: bool = False) -> _rpc_type:
        if self._is_capture:
            if arg is None:
                return self._device._capture(self.__name__, 5.0, raw)
            assert isinstance(arg, (int, float))
            return self._device._capture(self.__name__, float(arg), raw)

        match self._type:
            case t if t is bool:
                if arg is not None and not (isinstance(arg, bool) or arg in (0, 1)):
                    raise TypeError(f"{self.__name__} expects True or False")
                value = None if arg is None else int(arg)
                return bool(
                    self._device._rpc_int(
                        self.__name__, self._data_size or 1, False, value
                    )
                )
            case t if t is int:
                assert arg is None or isinstance(arg, int)
                assert self._data_size is not None
                return self._device._rpc_int(
                    self.__name__, self._data_size, self._signed, arg
                )
            case t if t is float:
                try:
                    value = None if arg is None else float(arg)
                except (TypeError, ValueError) as err:
                    raise TypeError(
                        f"{self.__name__} expects a float-compatible value"
                    ) from err
                assert self._data_size is not None
                return self._device._rpc_float(self.__name__, self._data_size, value)
            case t if t is str:
                if arg is None:
                    arg = ""
                assert isinstance(arg, str)
                return self._device._rpc(self.__name__, arg.encode()).decode()
            case t if t is bytes:
                if arg is None:
                    arg = b""
                assert isinstance(arg, bytes)
                return self._device._rpc(self.__name__, arg)
            case None:
                return self._device._rpc(self.__name__, b"")
            case other:
                raise TypeError(
                    f"Invalid RPC type {other}, RPC types must be {_rpc_type}"
                )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Calls the RPC; each kind of RPC defines its arguments."""
        raise NotImplementedError


class _RpcReadOnly(_Rpc):
    def __call__(self, timeout: float | None = None, *, raw: bool = False):
        if self._is_capture:
            return self._call(timeout, raw=raw)
        if timeout is not None or raw:
            raise TypeError(
                f"{self.__name__} is read-only and does not accept an argument"
            )
        return self._call()


class _RpcWriteOnly(_Rpc):
    def __call__(self, arg):
        return self._call(arg)


class _RpcReadWrite(_Rpc):
    def __call__(self, arg=None):
        return self._call(arg)


class _RpcAction(_Rpc):
    def __call__(self) -> None:
        self._call()


class _RpcCapture(_Rpc):
    def __repr__(self):
        return f"{self.__module__}.{self.__class__.__name__}('{self.__name__}', capture=True)"

    def __call__(self, timeout: float = 5.0, *, raw: bool = False) -> dict:
        return self._device._capture(self.__name__, timeout, raw)


# Samples classes
class _SamplesBase:
    """Base class for sample objects"""

    def __init__(self, device: Device, name: str, stream: str, columns: list[str]):
        self._device = device
        self.__name__ = name
        self._stream = stream
        self._columns = columns

    def __repr__(self):
        return f"{self.__module__}.{self.__class__.__name__}('{self.__name__}', stream='{self._stream}', columns={self._columns})"


class _SamplesDict(_SamplesBase):
    """Returns samples as dict keyed by stream_id"""

    def __init__(
        self,
        device: Device,
        name: str,
        stream: str = "",
        columns: list[str] | None = None,
    ):
        super().__init__(device, name, stream, columns if columns is not None else [])

    def __call__(self, n: int = 1):
        return self._device._samples_dict(n, self._stream, self._columns)


class _SamplesList(_SamplesBase):
    """Returns samples as list for single stream"""

    def __init__(
        self,
        device: Device,
        name: str,
        stream: str = "",
        columns: list[str] | None = None,
    ):
        super().__init__(device, name, stream, columns if columns is not None else [])

    def __call__(self, n: int = 1, *, time_column=True, title_row=True):
        return self._device._samples_list(
            n, self._stream, self._columns, time_column=time_column, title_row=title_row
        )


# Device trees
class DeviceInfo(NamedTuple):
    """A device that discovery found: its route, `dev.name` and `dev.serial`.

    `name` and `serial` are None when the device did not answer.
    """

    route: str
    name: str | None
    serial: str | None


def _url_key(url: str | Sequence[str] | None) -> tuple[str, ...]:
    if url is None:
        return ()
    if isinstance(url, str):
        return (url,)
    return tuple(url)


# One live Tree per URL, so every Device opened on a URL shares its connection.
# The cache holds them weakly: a tree closes once no Tree or Device uses it.
_trees: "weakref.WeakValueDictionary[tuple[str, ...], Tree]" = (
    weakref.WeakValueDictionary()
)


def connect(url: str | Sequence[str] | None = None, *, discover: float = 2.0) -> "Tree":
    """Connects to a device tree and returns it.

    `url` names a device, such as `serial:///dev/ttyACM0`, or the default
    device when it is None. A list of URLs mounts each device at `/0`, `/1`,
    and so on, as one tree. Calls with the same `url` return the same Tree
    while it is in use.

    Discovery listens for `discover` seconds for the devices of the tree. It
    runs once per tree; `Tree.refresh()` runs it again.
    """
    key = _url_key(url)
    tree = _trees.get(key)
    if tree is None:
        tree = Tree(url)
        _trees[key] = tree
    if discover and not tree._discovered:
        tree.refresh(discover)
    return tree


class Tree:
    """Every device reachable through one connection, such as a hub and the
    sensors behind it.

    Use `twinleaf.connect(url)` to get one. Index it by route to get a
    `Device`: `tree["/0"]`. The devices share the tree's connection.
    """

    def __init__(self, url: str | Sequence[str] | None = None):
        key = _url_key(url)
        self._tree = _twinleaf._Tree(list(key) if key else None)
        self._info: dict[str, DeviceInfo] = {}
        self._devices: dict[str, Device] = {}
        self._discovered = False

    @property
    def url(self) -> str:
        """The URL, or URLs, the tree was opened with, or 'auto'."""
        return self._tree._url

    def refresh(self, seconds: float = 2.0) -> list[DeviceInfo]:
        """Listens for `seconds` for the tree's devices, and asks each for its
        name and serial. Returns what it found."""
        found = self._tree._discover(seconds)
        self._info = {
            route: DeviceInfo(route, name, serial) for route, name, serial in found
        }
        self._discovered = True
        return self.info

    @property
    def info(self) -> list[DeviceInfo]:
        """The devices found, in route order, without opening them."""
        return [self._info[route] for route in self.routes]

    @property
    def routes(self) -> list[str]:
        """The routes of the devices found, in order."""
        return sorted(self._info, key=_route_sort_key)

    def __getitem__(self, route: str) -> Device:
        """Returns the device at `route`, such as "/0", opening it on first use."""
        route = _twinleaf._parse_route(route)
        if route not in self._info:
            raise KeyError(
                f"no device found at {route} in {self.url}; found {self.routes}. "
                "Tree.refresh() looks again."
            )
        device = self._devices.get(route)
        if device is None:
            device = Device(route=route, tree=self)
            self._devices[route] = device
        return device

    def __contains__(self, route: object) -> bool:
        if not isinstance(route, str):
            return False
        try:
            return _twinleaf._parse_route(route) in self._info
        except ValueError:
            return False

    def __iter__(self) -> Iterator[Device]:
        """Opens and yields every device found, in route order."""
        for route in self.routes:
            yield self[route]

    def __len__(self) -> int:
        return len(self._info)

    def find(self, name: str | None = None, serial: str | None = None) -> list[Device]:
        """Returns the devices whose `dev.name` (ignoring case) and `dev.serial`
        match every criterion given, in route order."""
        matches = []
        for info in self.info:
            if name is not None and (info.name or "").lower() != name.lower():
                continue
            if serial is not None and info.serial != serial:
                continue
            matches.append(self[info.route])
        return matches

    def acquire(
        self, streams: Sequence[str] | None = None, *, retention: float = 60.0
    ) -> Acquisition:
        """Starts buffering every stream of the tree, or only `streams`, such
        as `["/0/vector", "/imu"]`, keeping the latest `retention` seconds of
        each. Use it as a context manager, or close it when done:

            with tree.acquire() as acq:
                acq.wait(5)
                s = acq.window(5)   # the same 5 s of device time from every stream
        """
        return Acquisition(self, streams, retention)

    def samples(
        self,
        seconds: float,
        streams: Sequence[str] | None = None,
        *,
        timeout: float | None = None,
        allow_mixed_timebases: bool = False,
    ) -> Slice:
        """Acquires `seconds` of every stream, or of `streams`, and returns
        them as one `Slice` over the same interval of device time. See
        `Acquisition.window()`."""
        with self.acquire(streams, retention=max(2 * seconds, seconds + 10)) as acq:
            acq.wait(seconds, timeout=timeout)
            return acq.window(seconds, allow_mixed_timebases=allow_mixed_timebases)

    def unify(self, devices: str | Sequence[str | Device]) -> "Unified":
        """Returns several devices addressed as one: every device whose
        `dev.name` is `devices`, such as "VMR", or the devices or routes
        listed."""
        if isinstance(devices, str):
            members = self.find(name=devices)
            if not members:
                names = sorted({info.name for info in self.info if info.name})
                raise KeyError(
                    f"no device named '{devices}' in {self.url}; names found: {names}"
                )
        else:
            members = [d if isinstance(d, Device) else self[d] for d in devices]
        return Unified(members)

    def __repr__(self) -> str:
        lines = [f"{self.__module__}.{self.__class__.__name__}('{self.url}')"]
        width = max((len(info.route) for info in self.info), default=0)
        name_width = max((len(info.name or "?") for info in self.info), default=0)
        for info in self.info:
            name = info.name or "?"
            lines.append(
                f"  {info.route:<{width}}  {name:<{name_width}}  {info.serial or ''}".rstrip()
            )
        if not self._info:
            lines.append("  (no devices found; Tree.refresh() looks again)")
        return "\n".join(lines)


def _route_sort_key(route: str) -> tuple[int, ...]:
    return tuple(int(hop) for hop in route.strip("/").split("/") if hop)


# Unified devices: several devices of one kind, addressed as one
class Mixed(dict):
    """The values of an RPC when the unified devices disagree, keyed by route."""

    def __repr__(self) -> str:
        return f"twinleaf.Mixed({dict.__repr__(self)})"


class UnifiedRpcError(RuntimeError):
    """An RPC failed on some of the unified devices. It was tried on every
    device; `results` holds the replies, and `errors` the failures, by route."""

    def __init__(
        self, message: str, results: dict[str, Any], errors: dict[str, Exception]
    ):
        super().__init__(message)
        self.results = results
        self.errors = errors


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, float) and isinstance(b, float) and a != a and b != b:
        return True  # both NaN
    return type(a) is type(b) and a == b


def _combine(results: dict[str, Any]) -> Any:
    """One value when every device agrees, or else a Mixed mapping."""
    values = list(results.values())
    if all(_same(values[0], value) for value in values[1:]):
        return values[0]
    return Mixed(results)


class _UnifiedNode(_RpcNode):
    """A node of a unified settings tree."""

    def _survey(self) -> dict[str, Any]:
        results = {}
        for attr in self.__dict__.values():
            if isinstance(attr, _UnifiedRpc):
                first = attr._first
                if (
                    first._readable
                    and first._type not in {None, bytes}
                    and not first._is_capture
                ):
                    results[attr.__name__] = attr()
            if isinstance(attr, _UnifiedNode):
                results |= attr._survey()
        return results


class _UnifiedSurvey(_UnifiedNode):
    """A branch of a unified settings tree. Calling it reads every readable
    setting below it from every device."""

    def __call__(self) -> dict[str, Any]:
        return self._survey()


class _UnifiedRpc(_UnifiedNode):
    """One RPC of every unified device. It takes the same arguments as the
    RPC of one device, calls it on each, and returns the one value they all
    agree on, or a `Mixed` mapping of route to value when they differ.
    Captures return a dict of route to capture."""

    def __init__(self, name: str, members: list[tuple[str, "_Rpc"]]):
        super().__init__(name)
        self._members = members
        self._first = members[0][1]

    def __repr__(self) -> str:
        detail = repr(self._first).split("(", 1)[1].rstrip(")")
        routes = [route for route, _ in self._members]
        return (
            f"{self.__module__}.{self.__class__.__name__}({detail}, devices={routes})"
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        results: dict[str, Any] = {}
        errors: dict[str, Exception] = {}
        for route, rpc in self._members:
            try:
                results[route] = rpc(*args, **kwargs)
            # Every device is tried; failures are raised together afterwards.
            except Exception as error:
                errors[route] = error
        if errors:
            first = next(iter(errors.values()))
            if not results and all(
                type(error) is type(first) and str(error) == str(first)
                for error in errors.values()
            ):
                # The same failure everywhere, such as an argument of the wrong
                # type: it is not about any one device.
                raise first
            failed = "; ".join(f"{route}: {error}" for route, error in errors.items())
            done = f"; it succeeded on {', '.join(results)}" if results else ""
            raise UnifiedRpcError(
                f"{self.__name__} failed on {failed}{done}", results, errors
            )
        if isinstance(self._first, _RpcAction):
            return None
        if self._first._is_capture:
            return results
        return _combine(results)


class Unified:
    """Several devices of one kind, addressed as one, like the Twinleaf
    app's "Unify Matching Sensors".

    Use `tree.unify("VMR")` to get one. `settings` holds the RPCs every
    device has, with the same type: calling one reads or writes it on every
    device. `samples()` reads the same interval of each device's streams.
    """

    def __init__(self, devices: Sequence[Device]):
        if not devices:
            raise ValueError("unify needs at least one device")
        devices = sorted(devices, key=lambda d: _route_sort_key(d._route))
        trees = {id(device.tree) for device in devices}
        if len(trees) != 1:
            raise ValueError("unified devices must belong to one tree")
        self.tree: Tree = devices[0].tree
        self.devices: list[Device] = list(devices)
        self.settings = _UnifiedSurvey("settings")
        self.not_shared: list[str] = []
        self._build_settings()

    @property
    def routes(self) -> list[str]:
        """The routes of the unified devices, in order."""
        return [device._route for device in self.devices]

    def _build_settings(self) -> None:
        tables = []
        for device in self.devices:
            names = device._registry.suggest("")
            tables.append({name: device._registry.find(name) for name in names})
        every = set().union(*tables)
        shared = sorted(
            name
            for name in every
            if all(name in table for table in tables)
            and len({table[name].meta_raw for table in tables}) == 1
        )
        # RPCs some devices lack, or that differ in type or access between
        # them, cannot be called as one.
        self.not_shared = sorted(every - set(shared))
        nodes: dict[str, _UnifiedNode] = {"": self.settings}
        for name in shared:
            parent = ""
            for part in name.split(".")[:-1]:
                path = f"{parent}.{part}" if parent else part
                if path not in nodes:
                    node = _UnifiedSurvey("_rpc" if part == "rpc" else part)
                    setattr(nodes[parent], "_rpc" if part == "rpc" else part, node)
                    nodes[path] = node
                parent = path
            leaf = name.rsplit(".", 1)[-1]
            members = [
                (device._route, _Rpc(table[name], device))
                for device, table in zip(self.devices, tables)
            ]
            rpc = _UnifiedRpc(name, members)
            existing = nodes.get(name)
            if existing is not None:
                # A survey made for longer names already: keep its children.
                rpc.__dict__.update(
                    {
                        k: v
                        for k, v in existing.__dict__.items()
                        if isinstance(v, _UnifiedNode)
                    }
                )
            setattr(nodes[parent], "_rpc" if leaf == "rpc" else leaf, rpc)
            nodes[name] = rpc

    def stream_paths(self, *streams: str) -> list[str]:
        """The paths of the named streams on every device, such as
        `["/0/vector", "/1/vector"]` for "vector". With no names, every stream
        all the devices have."""
        if not streams:
            per_device = [
                set(device._get_metadata()["streams"]) for device in self.devices
            ]
            streams = tuple(sorted(set.intersection(*per_device)))
            if not streams:
                raise ValueError(
                    f"the devices at {', '.join(self.routes)} have no stream in "
                    "common; name the streams to read"
                )
        paths = []
        for device in self.devices:
            for stream in streams:
                route = device._route
                paths.append(f"/{stream}" if route == "/" else f"{route}/{stream}")
        return paths

    def samples(
        self,
        seconds: float,
        streams: Sequence[str] | None = None,
        *,
        timeout: float | None = None,
        allow_mixed_timebases: bool = False,
    ) -> Slice:
        """Reads the same `seconds` of device time from the named streams of
        every device, such as `samples(5, ["vector"])`, or from every stream
        they all have. Devices behind different hubs count on different
        clocks; `allow_mixed_timebases=True` reads each clock's latest window.
        See `Tree.samples()`."""
        paths = self.stream_paths(*(streams or ()))
        return self.tree.samples(
            seconds, paths, timeout=timeout, allow_mixed_timebases=allow_mixed_timebases
        )

    def __iter__(self) -> Iterator[Device]:
        return iter(self.devices)

    def __len__(self) -> int:
        return len(self.devices)

    def __repr__(self) -> str:
        members = ", ".join(f"{d._route}" for d in self.devices)
        noun = "device" if len(self.devices) == 1 else "devices"
        return f"twinleaf.Unified({len(self.devices)} {noun} at {members})"
