"""Synchronized acquisition across the devices of a tree.

An `Acquisition` subscribes once to every stream of a tree and buffers each
in the background. `window()` and `slice()` cut the same interval of device
time out of every stream, after checking that the streams count time on the
same clock.
"""

import time as _time
import warnings
from collections.abc import Iterable, Iterator
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


class StreamSlice:
    """One stream's rows within a slice.

    `time` holds each row's end-of-sample time in seconds of the stream's
    `timebase`. Index by column name, with or without the stream prefix:
    `s["x"]` or `s["vector.x"]`.
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
        return (
            f"twinleaf.StreamSlice('{self.path}', {len(self)} rows at {self.rate_hz:g} Hz, "
            f"columns={self.columns})"
        )


class Slice:
    """The same interval of device time from several streams.

    Index by stream path, such as `s["/0/vector"]` or `s["/imu"]`, or by
    stream name when only one stream has it: `s["vector"]`. Each stream keeps
    its own rate and its own `time` array; nothing is resampled.
    """

    def __init__(
        self, streams: list[StreamSlice], host_offsets: dict[Timebase, float | None]
    ):
        self._streams = {stream.path: stream for stream in streams}
        self._host_offsets = host_offsets

    def __getitem__(self, path: str) -> StreamSlice:
        if path in self._streams:
            return self._streams[path]
        named = [s for s in self._streams.values() if s.stream == path.strip("/")]
        if len(named) == 1:
            return named[0]
        try:
            canonical = _stream_path(path)
        except ValueError:
            canonical = path
        if canonical in self._streams:
            return self._streams[canonical]
        raise KeyError(
            f"no stream '{path}' in this slice; its streams are {self.paths}"
        )

    def __iter__(self) -> Iterator[StreamSlice]:
        return iter(self._streams.values())

    def __len__(self) -> int:
        return len(self._streams)

    @property
    def paths(self) -> list[str]:
        """The stream paths, in order."""
        return list(self._streams)

    @property
    def timebases(self) -> list[Timebase]:
        """Every timebase among the streams, one unless mixed timebases were allowed."""
        return sorted({stream.timebase for stream in self})

    @property
    def timebase(self) -> Timebase:
        """The streams' common timebase. Raises if they have several."""
        timebases = self.timebases
        if len(timebases) != 1:
            raise TimebaseMismatch(
                f"this slice mixes timebases: {[str(t) for t in timebases]}"
            )
        return timebases[0]

    @property
    def start(self) -> float:
        """The start of the slice's interval; rows end after it."""
        _ = self.timebase  # raises when the streams mix timebases
        return next(iter(self)).start

    @property
    def end(self) -> float:
        """The end of the slice's interval; rows end at or before it."""
        _ = self.timebase  # raises when the streams mix timebases
        return next(iter(self)).end

    @property
    def gaps(self) -> list[Gap]:
        """Samples lost within the slice, from every stream."""
        return sorted(
            (gap for stream in self for gap in stream.gaps), key=lambda g: g.after
        )

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
        for stream in self:
            lines.append(
                f"  {stream.path}: {len(stream)} rows at {stream.rate_hz:g} Hz, {stream.columns}"
            )
        for timebase in self.timebases:
            members = [s for s in self if s.timebase == timebase]
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
        streams: Iterable[str] | None = None,
        retention: float = 60.0,
    ):
        self.tree = tree
        self.retention = float(retention)
        if not tree._discovered:
            tree.refresh()
        selected = None if streams is None else [_stream_path(p) for p in streams]
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
    def _infos(self, streams: Iterable[str] | None) -> list[dict]:
        infos = {info["path"]: info for info in self._acq._streams()}
        if streams is None:
            return list(infos.values())
        selected = []
        for path in streams:
            canonical = _stream_path(path)
            if canonical not in infos:
                raise KeyError(
                    f"no stream '{path}' in this acquisition; its streams are {list(infos)}"
                )
            selected.append(infos[canonical])
        return selected

    @property
    def streams(self) -> list[str]:
        """The paths of every stream being buffered."""
        return [info["path"] for info in self._acq._streams()]

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
        streams: Iterable[str] | None = None,
        timeout: float | None = None,
    ) -> None:
        """Waits until the latest `seconds` of every stream are buffered, so
        that `window(seconds)` succeeds. Raises `NotEnoughData` after
        `timeout` seconds, by default `seconds` + 10."""
        self._check_seconds(seconds)
        streams = list(streams) if streams is not None else None
        deadline = _time.monotonic() + (seconds + 10.0 if timeout is None else timeout)
        while True:
            infos = self._infos(streams)
            shortfall = self._shortfall(infos, seconds)
            if shortfall is None:
                return
            self._check_running()
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
        streams: Iterable[str] | None = None,
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
        is buffered yet; `wait(seconds)` waits for it.
        """
        self._check_seconds(seconds)
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
        streams: Iterable[str] | None = None,
        *,
        trim: str | None = None,
    ) -> Slice:
        """Returns the rows of every stream whose end-of-sample time lies in
        `(start, end]`, in device seconds of the streams' common timebase.

        Raises `NotEnoughData` unless every stream covers the whole interval.
        `trim="overlap"` cuts the interval to the part every stream covers
        instead. Raises `TimebaseMismatch` when the streams count on different
        clocks.
        """
        if not end > start:
            raise ValueError("end must be after start")
        if trim not in (None, "overlap"):
            raise ValueError("trim must be None or 'overlap'")
        infos = self._infos(streams)
        empty = [info["path"] for info in infos if info["rows"] == 0]
        if empty:
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
                streams.append(
                    StreamSlice(
                        info, self._acq._slice(info["path"], start, end), start, end
                    )
                )
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
                    stacklevel=4,
                )

    def __repr__(self) -> str:
        state = "closed" if self._closed else "running"
        return (
            f"twinleaf.Acquisition('{self.tree.url}', {state}, streams={self.streams})"
        )
