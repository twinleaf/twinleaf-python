"""Synchronized acquisition across the devices of a tree.

An `Acquisition` subscribes once to every stream of a tree and buffers each
in the background. `window()` and `slice()` cut the same interval of device
time out of every stream, after checking that the streams count time on the
same clock.
"""

import copy
import os
import time as _time
import warnings
from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np

from twinleaf import _twinleaf
from twinleaf._twinleaf import SampleLossWarning

if TYPE_CHECKING:
    from twinleaf import Tree

# Device times are seconds of uptime or of the UNIX epoch, in float64, so
# comparisons allow for rounding at that scale.
_EPSILON = 1e-6


class TimebaseMismatch(ValueError):
    """The streams asked for do not count time on the same clock, so their
    times cannot be compared."""


class NotEnoughData(RuntimeError):
    """The buffered data does not cover the interval asked for."""


class Timebase(NamedTuple):
    """The clock a stream's times count on: an epoch (`systime`, the uptime
    of the device that owns the clock, or `unix`), and that device's serial
    and session. Streams with equal timebases can be compared by time."""

    epoch: str
    serial: str
    session: int

    def __str__(self) -> str:
        return f"{self.epoch} time of {self.serial} (session {self.session})"


class Gap(NamedTuple):
    """Samples lost from `stream` between the rows at times `after` and
    `before`. `dropped_batches` is nonzero when the acquisition fell behind,
    and zero when the device or its link lost them."""

    stream: str
    after: float
    before: float
    samples: int
    dropped_batches: int


def _period(info: dict) -> float:
    """The stream's sample period, or 0 when its rate is unknown."""
    rate = info["rate_hz"]
    return 1.0 / rate if rate and rate > 0 else 0.0


def _stream_path(path: str) -> str:
    """Returns a stream path in canonical form: `/imu` for stream `imu` at
    the root, `/0/vector` for stream `vector` at route `/0`."""
    parts = path.strip().strip("/").split("/")
    if not parts or not parts[-1]:
        raise ValueError(f"invalid stream path '{path}': expected /route/stream")
    route = _twinleaf._parse_route("/" + "/".join(parts[:-1]))
    stream = parts[-1]
    return f"/{stream}" if route == "/" else f"{route}/{stream}"


def _find_stream(name: str, streams: Mapping[str, str], where: str) -> str:
    """Returns the path of the stream `name` asks for among `streams`, a
    mapping of path to stream name. A name with no slash, such as `vector`,
    is the one stream with that name; anything else is a path, such as
    `/0/vector`, or `/imu` for a stream of the root device."""
    name = name.strip()
    if "/" not in name:
        named = [path for path, stream in streams.items() if stream == name]
        if len(named) == 1:
            return named[0]
        if named:
            raise KeyError(
                f"several streams are named '{name}' in {where}: {named}; give its path"
            )
        raise KeyError(
            f"no stream named '{name}' in {where}; its streams are {list(streams)}"
        )
    try:
        canonical = _stream_path(name)
    except ValueError:
        canonical = name
    if canonical in streams:
        return canonical
    raise KeyError(f"no stream '{name}' in {where}; its streams are {list(streams)}")


def _stream_list(streams: str | Iterable[str] | None) -> list[str] | None:
    """The streams asked for as a list; one name alone is one stream."""
    if streams is None:
        return None
    if isinstance(streams, str):
        return [streams]
    return list(streams)


class StreamSlice:
    """One stream's rows within a slice.

    `time` holds each row's end-of-sample time in seconds of the stream's
    `timebase`. Index by column name, with or without the stream prefix:
    `s["x"]` or `s["vector.x"]`. The arrays are read-only; copy one to
    change it.
    """

    def __init__(self, info: dict, raw: dict, start: float, end: float):
        self.path: str = info["path"]
        self.route: str = info["route"]
        self.stream: str = info["stream"]
        self.timebase = Timebase(*info["timebase"])
        self.rate_hz: float = info["rate_hz"]
        self.start = start
        self.end = end
        self.time = np.frombuffer(raw["time"], dtype="<f8")
        self._columns: dict[str, np.ndarray] = {}
        self.units: dict[str, str] = {}
        for name, dtype, units, data in raw["columns"]:
            self._columns[name] = np.frombuffer(data, dtype=dtype)
            self.units[name] = units
        self.gaps = [Gap(self.path, *gap[1:]) for gap in raw["gaps"]]
        self._gap_ids = [gap[0] for gap in raw["gaps"]]
        # Seconds, when the slice was cut, since the stream's newest row arrived.
        self.age: float | None = raw.get("idle")
        # How the rows were resampled onto another stream's times, or None
        # for the stream's own rows.
        self.resampled: str | None = None

    def _derived(
        self, time: np.ndarray, columns: dict[str, np.ndarray], resampled: str | None
    ) -> "StreamSlice":
        """A copy with other rows, keeping the stream's description."""
        derived = copy.copy(self)
        derived.units = dict(self.units)
        derived.gaps = list(self.gaps)
        # Read-only, as the arrays of a slice that was not resampled are.
        for array in (time, *columns.values()):
            array.flags.writeable = False
        derived.time = time
        derived._columns = columns
        derived.resampled = resampled
        return derived

    @property
    def columns(self) -> list[str]:
        """The column names, in the stream's order."""
        return list(self._columns)

    def __getitem__(self, name: str) -> np.ndarray:
        if name in self._columns:
            return self._columns[name]
        prefixed = f"{self.stream}.{name}"
        if prefixed in self._columns:
            return self._columns[prefixed]
        raise KeyError(
            f"no column '{name}' in {self.path}; its columns are {self.columns}"
        )

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and (
            name in self._columns or f"{self.stream}.{name}" in self._columns
        )

    def __len__(self) -> int:
        return len(self.time)

    def to_dict(self) -> dict[str, np.ndarray]:
        """The rows as a dict of arrays, `time` first."""
        return {"time": self.time, **self._columns}

    def __repr__(self) -> str:
        how = f", resampled ({self.resampled})" if self.resampled else ""
        return (
            f"twinleaf.StreamSlice('{self.path}', {len(self)} rows, {self.rate_hz:g} Hz source{how}, "
            f"columns={self.columns})"
        )


_RESAMPLE_METHODS = ("previous", "linear")


def _defined(source: StreamSlice, times: np.ndarray, method: str) -> np.ndarray:
    """Where `source` defines a value at `times` by `method`: after its first
    row for "previous", within its first and last rows for "linear", and in
    neither case inside one of its gaps."""
    t = source.time
    if len(t) == 0:
        return np.zeros(len(times), dtype=bool)
    if method == "previous":
        defined = times >= t[0]
    else:
        defined = (times >= t[0]) & (times <= t[-1])
    for gap in source.gaps:
        defined &= ~((times > gap.after) & (times < gap.before))
    return defined


def _resample_column(
    t: np.ndarray,
    values: np.ndarray,
    times: np.ndarray,
    method: str,
    defined: np.ndarray,
    trimmed: bool,
) -> np.ndarray:
    """The dtype of the result depends on the column, the method and the
    edges only, never on the data."""
    if method == "previous":
        index = np.searchsorted(t, times, side="right") - 1
        if trimmed:
            return values[index]  # every time has a value: keep any dtype
        # A float column holds NaN in its own dtype; any other becomes float64.
        dtype = values.dtype if values.dtype.kind == "f" else np.float64
        out = np.full(len(times), np.nan, dtype=dtype)
        out[defined] = values[index[defined]]
        return out
    out = np.full(len(times), np.nan)
    if defined.any():
        out[defined] = np.interp(times[defined], t, values.astype(np.float64))
    return out


class Slice(Mapping[str, StreamSlice]):
    """The same interval of device time from several streams.

    A read-only mapping of stream path to `StreamSlice`: index it by path,
    such as `s["/0/vector"]` or `s["/imu"]`, or by a stream's name alone
    when only one stream has it: `s["vector"]`. Iterating gives the paths; `values()` and
    `items()` give the streams. Each stream keeps its own rate and its own
    `time` array, unless `resample()` puts them on one stream's times.
    """

    def __init__(
        self,
        streams: list[StreamSlice],
        host_offsets: dict[Timebase, float | None],
        aligned_on: str | None = None,
    ):
        self._streams = {stream.path: stream for stream in streams}
        self._host_offsets = host_offsets
        self._aligned_on = aligned_on

    @property
    def aligned_on(self) -> str | None:
        """The stream whose times every stream was resampled onto, or None
        for a slice whose streams keep their own times."""
        return self._aligned_on

    def resample(
        self,
        on: str,
        method: str | Mapping[str, str],
        *,
        edges: str = "nan",
    ) -> "Slice":
        """Returns the slice with every stream resampled onto the times of
        the stream `on`, such as `resample(on="/0/vector", method="previous")`.

        `method` says how to find each stream's value at a time, and has no
        default:

        - "previous" holds the latest row that ended at or before the time.
          This suits slow housekeeping data such as a barometer.
        - "linear" interpolates between the rows around the time. This suits
          signals sampled well above their bandwidth.

        A dict of stream to method sets it per stream. Streams are aligned by
        their end-of-sample times; device filter delays are not corrected.

        Where a stream has no value, the result is NaN: before its first row,
        after its last row for "linear", or inside one of its gaps.
        `edges="trim"` drops those times instead, so every stream has a value
        at every remaining time.

        - dtypes: "linear" gives float64. "previous" keeps a float column's
          dtype, and makes any other column float64 so that it can hold NaN.
          With `edges="trim"`, "previous" keeps every dtype, which also keeps
          64-bit integers exact.
        - A method applies to every column of its stream. "linear" also
          interpolates integer columns such as counters and status flags;
          use "previous" for a stream that has them.
        - Resampling a fast stream onto a slower one takes its value at each
          time and does not average, so a signal above half the slower rate
          aliases. Filter or decimate it on the device first.
        - Every stream keeps its source `rate_hz`, and its `resampled`
          attribute names the method. `start` and `end` stay the interval
          the slice was cut for, also after `edges="trim"`.
        """
        if edges not in ("nan", "trim"):
            raise ValueError("edges must be 'nan' or 'trim'")
        _ = self.timebase  # raises when the streams mix timebases
        reference = self[on]
        others = [stream for stream in self.values() if stream.path != reference.path]
        if isinstance(method, str):
            methods = {stream.path: method for stream in others}
        else:
            methods = {self[path].path: m for path, m in method.items()}
            missing = [s.path for s in others if s.path not in methods]
            if missing:
                raise ValueError(f"no resampling method given for {missing}")
        for path, m in methods.items():
            if m not in _RESAMPLE_METHODS:
                raise ValueError(
                    f"unknown method '{m}' for {path}: use 'previous' or 'linear'"
                )

        times = reference.time
        defined = {s.path: _defined(s, times, methods[s.path]) for s in others}
        keep = np.ones(len(times), dtype=bool)
        if edges == "trim":
            for mask in defined.values():
                keep &= mask
            if not keep.any():
                raise NotEnoughData(
                    f"no time of {reference.path} has a value from every stream"
                )
            times = times[keep]
        streams = []
        for stream in self.values():
            if stream.path == reference.path:
                columns = {
                    name: values[keep] for name, values in stream._columns.items()
                }
                streams.append(stream._derived(times.copy(), columns, None))
                continue
            m = methods[stream.path]
            mask = defined[stream.path][keep]
            columns = {
                name: _resample_column(
                    stream.time, values, times, m, mask, edges == "trim"
                )
                for name, values in stream._columns.items()
            }
            streams.append(stream._derived(times, columns, m))
        return Slice(streams, self._host_offsets, aligned_on=reference.path)

    def __getitem__(self, path: str) -> StreamSlice:
        names = {p: stream.stream for p, stream in self._streams.items()}
        return self._streams[_find_stream(path, names, "this slice")]

    def __contains__(self, path: object) -> bool:
        if not isinstance(path, str):
            return False
        try:
            self[path]
        except KeyError:
            return False
        return True

    def __iter__(self) -> Iterator[str]:
        return iter(self._streams)

    # Slices are compared by identity: their arrays have no single truth value.
    def __eq__(self, other: object) -> bool:
        return self is other

    __hash__ = object.__hash__

    def __len__(self) -> int:
        return len(self._streams)

    @property
    def paths(self) -> list[str]:
        """The stream paths, in order."""
        return list(self._streams)

    @property
    def timebases(self) -> list[Timebase]:
        """Every timebase among the streams, one unless mixed timebases were allowed."""
        return sorted({stream.timebase for stream in self.values()})

    @property
    def timebase(self) -> Timebase:
        """The streams' common timebase. Raises if they have several."""
        timebases = self.timebases
        if len(timebases) != 1:
            raise TimebaseMismatch(
                f"this slice mixes timebases: {[str(t) for t in timebases]}; "
                "by_timebase() splits it"
            )
        return timebases[0]

    def by_timebase(self) -> "dict[Timebase, Slice]":
        """The slice split by clock: each timebase with a slice of the
        streams that count on it. Each part has one timebase, so its `start`,
        `end` and `host_time_offset` are defined, which they are not for a
        slice that mixes timebases."""
        parts = {}
        for timebase in self.timebases:
            members = [s for s in self.values() if s.timebase == timebase]
            aligned = self._aligned_on
            if aligned is not None and all(s.path != aligned for s in members):
                aligned = None
            parts[timebase] = Slice(
                members, {timebase: self._host_offsets.get(timebase)}, aligned
            )
        return parts

    @property
    def start(self) -> float:
        """The start of the slice's interval; rows end after it."""
        _ = self.timebase  # raises when the streams mix timebases
        return next(iter(self.values())).start

    @property
    def end(self) -> float:
        """The end of the slice's interval; rows end at or before it."""
        _ = self.timebase  # raises when the streams mix timebases
        return next(iter(self.values())).end

    @property
    def gaps(self) -> list[Gap]:
        """Samples lost within the slice, from every stream."""
        return sorted(
            (gap for stream in self.values() for gap in stream.gaps),
            key=lambda g: g.after,
        )

    @property
    def age(self) -> float | None:
        """Seconds, when the slice was cut, since the newest row arrived of
        the stream heard from least recently.

        It is about the interval between batches of the slowest stream while
        every stream is running. It keeps growing once a stream stops, and the
        latest window then stays the same, as it ends where that stream ends.
        """
        ages = [stream.age for stream in self.values() if stream.age is not None]
        return max(ages) if ages else None

    @property
    def host_time_offset(self) -> float | None:
        """Seconds to add to `time` for an estimate of UNIX time.

        For a `unix` timebase it is 0. Otherwise it is the smallest difference
        between the host's clock and a stream's newest time seen during the
        acquisition, which overestimates the true offset by the shortest
        transport delay seen. None when no batch was timed.
        """
        return self._host_offsets[self.timebase]

    def __repr__(self) -> str:
        lines = ["twinleaf.Slice("]
        for stream in self.values():
            if stream.resampled:
                rows = (
                    f"{len(stream)} rows, {stream.resampled} from {stream.rate_hz:g} Hz"
                )
            else:
                rows = f"{len(stream)} rows at {stream.rate_hz:g} Hz"
            lines.append(f"  {stream.path}: {rows}, {stream.columns}")
        for timebase in self.timebases:
            members = [s for s in self.values() if s.timebase == timebase]
            lines.append(
                f"  ({members[0].start:.6f}, {members[0].end:.6f}] in {timebase}"
            )
        lines.append(")")
        return "\n".join(lines)


class Acquisition:
    """Buffers every stream of a tree from the moment it starts.

    Use `tree.acquire()` to start one, as a context manager or closing it
    with `close()`. `wait()` waits until enough is buffered; `window()` and
    `slice()` return the same interval of device time from every stream.
    """

    def __init__(
        self,
        tree: "Tree",
        streams: str | Iterable[str] | None = None,
        retention: float = 60.0,
    ):
        self.tree = tree
        self.retention = float(retention)
        if not tree._discovered:
            tree.refresh()
        streams = _stream_list(streams)
        # A name with no slash is the one stream of the tree with that name,
        # which the extension finds once it knows the tree's streams.
        selected = (
            None
            if streams is None
            else [p.strip() if "/" not in p else _stream_path(p) for p in streams]
        )
        self._acq = tree._tree._acquire(tree.routes, selected, self.retention)
        self._reported_gaps: set[int] = set()
        self._closed = False

    # Lifetime
    def close(self) -> None:
        """Stops buffering. What is buffered stays readable."""
        if not self._closed:
            self._acq._close()
            self._closed = True

    def __enter__(self) -> "Acquisition":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # What is buffered
    def _infos(self, streams: str | Iterable[str] | None) -> list[dict]:
        infos = {info["path"]: info for info in self._acq._streams()}
        if streams is None:
            if not infos:
                raise NotEnoughData(
                    f"no stream of {self.tree.url} is enabled; enable one on its "
                    "device, or name the streams to acquire"
                )
            return list(infos.values())
        names = {path: info["stream"] for path, info in infos.items()}
        selected = [
            infos[_find_stream(path, names, "this acquisition")]
            for path in _stream_list(streams) or []
        ]
        return selected

    @property
    def streams(self) -> list[str]:
        """The paths of every stream being buffered."""
        return [info["path"] for info in self._acq._streams()]

    @property
    def dropped_batches(self) -> int:
        """How many batches the acquisition dropped because it fell behind.
        Each stream's share is also listed among its slices' `gaps`."""
        return self._acq._status()[1]

    def _check_running(self) -> None:
        ended, _ = self._acq._status()
        if ended is not None:
            raise ConnectionError(
                f"{ended}, which ended the acquisition from {self.tree.url}"
            )

    @staticmethod
    def _groups(infos: list[dict]) -> dict[Timebase, list[dict]]:
        groups: dict[Timebase, list[dict]] = {}
        for info in infos:
            if info["rows"] == 0 or info["timebase"] is None:
                continue
            groups.setdefault(Timebase(*info["timebase"]), []).append(info)
        return groups

    @staticmethod
    def _describe_groups(groups: dict[Timebase, list[dict]]) -> str:
        return "; ".join(
            f"{timebase}: {', '.join(info['path'] for info in members)}"
            for timebase, members in groups.items()
        )

    def _shortfall(self, infos: list[dict], seconds: float) -> str | None:
        """Says why the latest `seconds` are not yet buffered for every
        stream, or returns None when they are."""
        empty = [info["path"] for info in infos if info["rows"] == 0]
        if empty:
            disabled = [info["path"] for info in infos if not info["active"]]
            if disabled:
                return f"{', '.join(disabled)} is not enabled on its device"
            return f"no samples yet from {', '.join(empty)}"
        for members in self._groups(infos).values():
            end = min(info["last_end"] for info in members)
            start = end - seconds
            for info in members:
                if info["first_begin"] > start + _EPSILON:
                    covered = end - info["first_begin"]
                    run_break = info["run_break"]
                    if run_break is not None and run_break[0] > start:
                        return (
                            f"{info['path']} has data only from {info['first_begin']:.6f}, "
                            f"because {run_break[1]} then"
                        )
                    return f"{info['path']} has {max(covered, 0):.3f} s of the {seconds:g} s asked for"
        return None

    def wait(
        self,
        seconds: float,
        streams: str | Iterable[str] | None = None,
        timeout: float | None = None,
    ) -> None:
        """Waits until the latest `seconds` of every stream are buffered, so
        that `window(seconds)` succeeds. Raises `NotEnoughData` after
        `timeout` seconds, by default `seconds` + 10."""
        self._check_seconds(seconds)
        streams = _stream_list(streams)
        deadline = _time.monotonic() + (seconds + 10.0 if timeout is None else timeout)
        while True:
            infos = self._infos(streams)
            shortfall = self._shortfall(infos, seconds)
            if shortfall is None:
                return
            self._check_running()
            if self._closed:
                raise NotEnoughData(
                    f"the acquisition is closed, and {shortfall}; no more will arrive"
                )
            if _time.monotonic() >= deadline:
                raise NotEnoughData(
                    f"waited for {seconds:g} s of every stream, but {shortfall}. "
                    "Select the streams to wait for with streams=[...]."
                )
            _time.sleep(0.05)

    def _check_seconds(self, seconds: float) -> None:
        if not seconds > 0:
            raise ValueError("seconds must be positive")
        if seconds > self.retention:
            raise ValueError(
                f"{seconds:g} s is longer than the acquisition keeps ({self.retention:g} s); "
                "start it with a longer retention"
            )

    # Slices
    def window(
        self,
        seconds: float,
        streams: str | Iterable[str] | None = None,
        *,
        allow_mixed_timebases: bool = False,
    ) -> Slice:
        """Returns the latest `seconds` that every stream has.

        The window ends at the newest time that every selected stream has
        reached, so that slow, batched streams are complete up to its end.
        Rows are those whose end-of-sample time lies in `(end - seconds, end]`.
        Raises `TimebaseMismatch` when the streams count on different clocks,
        unless `allow_mixed_timebases`, which returns each clock's streams over
        that clock's own latest window. Raises `NotEnoughData` when not enough
        is buffered yet; `wait(seconds)` waits for it. Raises
        `ConnectionError` once the connection is lost, as the latest window
        would never change again. `Slice.age` tells a window that has stopped
        moving because a stream stopped.
        """
        self._check_seconds(seconds)
        self._check_running()
        infos = self._infos(streams)
        shortfall = self._shortfall(infos, seconds)
        if shortfall is not None:
            raise NotEnoughData(f"{shortfall}; wait({seconds:g}) waits for it")
        groups = self._groups(infos)
        if len(groups) > 1 and not allow_mixed_timebases:
            raise TimebaseMismatch(
                "these streams count time on different clocks, so their times cannot be "
                f"compared: {self._describe_groups(groups)}. Select streams of one clock, "
                "or pass allow_mixed_timebases=True."
            )
        intervals = {}
        for timebase, members in groups.items():
            end = min(info["last_end"] for info in members)
            intervals[timebase] = (end - seconds, end)
        return self._cut(groups, intervals)

    def slice(
        self,
        start: float,
        end: float,
        streams: str | Iterable[str] | None = None,
        *,
        trim: str | None = None,
    ) -> Slice:
        """Returns the rows of every stream whose end-of-sample time lies in
        `(start, end]`, in device seconds of the streams' common timebase.

        Raises `NotEnoughData` unless every stream covers the whole interval.
        `trim="overlap"` cuts the interval to the part every stream covers
        instead. Raises `TimebaseMismatch` when the streams count on different
        clocks. An interval that is buffered stays readable after the
        connection is lost; one that is not raises `ConnectionError` then.
        """
        if not end > start:
            raise ValueError("end must be after start")
        if trim not in (None, "overlap"):
            raise ValueError("trim must be None or 'overlap'")
        infos = self._infos(streams)
        empty = [info["path"] for info in infos if info["rows"] == 0]
        if empty:
            self._check_running()
            raise NotEnoughData(f"no samples yet from {', '.join(empty)}")
        groups = self._groups(infos)
        if len(groups) > 1:
            raise TimebaseMismatch(
                "these streams count time on different clocks, so one interval cannot "
                f"apply to all of them: {self._describe_groups(groups)}"
            )
        ((timebase, members),) = groups.items()
        if trim == "overlap":
            start = max(start, *(info["first_begin"] for info in members))
            end = min(end, *(info["last_end"] + _period(info) for info in members))
            if not end > start:
                raise NotEnoughData(
                    "the streams have no interval in common within the one asked for"
                )
        for info in members:
            if info["first_begin"] > start + _EPSILON:
                raise NotEnoughData(
                    f"{info['path']} has data only from {info['first_begin']:.6f}, after {start:.6f}"
                )
            # The next row of the stream would end after `end`, so none is missing.
            if info["last_end"] + _period(info) <= end - _EPSILON:
                self._check_running()
                raise NotEnoughData(
                    f"{info['path']} has data only up to {info['last_end']:.6f}, before {end:.6f}"
                )
        return self._cut(groups, {timebase: (start, end)})

    def _cut(
        self,
        groups: dict[Timebase, list[dict]],
        intervals: dict[Timebase, tuple[float, float]],
    ) -> Slice:
        streams = []
        requests: list[tuple[dict, float, float]] = []
        host_offsets: dict[Timebase, float | None] = {}
        for timebase, members in groups.items():
            start, end = intervals[timebase]
            offsets = [
                info["host_offset"]
                for info in members
                if info["host_offset"] is not None
            ]
            host_offsets[timebase] = (
                0.0 if timebase.epoch == "unix" else (min(offsets) if offsets else None)
            )
            for info in members:
                requests.append((info, start, end))
        # One call cuts every stream at the same moment. Each cut says what
        # its stream held then, which may be less than `groups` saw.
        cuts = self._acq._slices(
            [(info["path"], start, end) for info, start, end in requests]
        )
        for (info, start, end), cut in zip(requests, cuts):
            first_begin = cut["first_begin"]
            if cut["run"] != info["run"]:
                raise NotEnoughData(
                    f"{info['path']} restarted while the slice was being cut"
                )
            if first_begin is None or first_begin > start + _EPSILON:
                raise NotEnoughData(
                    f"{info['path']} no longer keeps data from {start:.6f}; "
                    "start the acquisition with a longer retention"
                )
            streams.append(StreamSlice(info, cut, start, end))
        self._warn_gaps(streams)
        return Slice(streams, host_offsets)

    def _warn_gaps(self, streams: list[StreamSlice]) -> None:
        for stream in streams:
            for gap_id, gap in zip(stream._gap_ids, stream.gaps):
                if gap_id in self._reported_gaps:
                    continue
                self._reported_gaps.add(gap_id)
                if gap.dropped_batches:
                    cause = f"the acquisition fell behind and dropped {gap.dropped_batches} batches"
                else:
                    cause = "the device or its link dropped them"
                warnings.warn(
                    f"{gap.stream} lost {gap.samples} samples between {gap.after:.6f} and "
                    f"{gap.before:.6f}: {cause}",
                    SampleLossWarning,
                    # Attributed to the caller's line, however it got here.
                    skip_file_prefixes=(os.path.dirname(__file__),),
                )

    def __repr__(self) -> str:
        state = "closed" if self._closed else "running"
        return (
            f"twinleaf.Acquisition('{self.tree.url}', {state}, streams={self.streams})"
        )
