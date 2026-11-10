"""Synchronized acquisition across the devices of a tree.

An `Acquisition` subscribes once to every stream of a tree and buffers each
in the background. `window()` and `slice()` cut the same interval of device
time out of every stream, after checking that the streams count time on the
same clock.
"""

import copy
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
        # How the rows were resampled onto another stream's times, or None
        # for the stream's own rows.
        self.resampled: str | None = None

    def _derived(
        self, time: np.ndarray, columns: dict[str, np.ndarray], resampled: str | None
    ) -> "StreamSlice":
        """A copy with other rows, keeping the stream's description."""
        derived = copy.copy(self)
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

    def to_pandas(self) -> Any:
        """The rows as a pandas DataFrame indexed by `time`. Needs pandas."""
        pd = _pandas()
        return pd.DataFrame(self._columns, index=pd.Index(self.time, name="time"))

    def __repr__(self) -> str:
        how = f", resampled ({self.resampled})" if self.resampled else ""
        return (
            f"twinleaf.StreamSlice('{self.path}', {len(self)} rows, {self.rate_hz:g} Hz source{how}, "
            f"columns={self.columns})"
        )


def _pandas() -> Any:
    try:
        import pandas
    except ImportError as error:
        raise ImportError(
            "to_pandas() needs pandas: pip install pandas, or pip install 'twinleaf[pandas]'"
        ) from error
    return pandas


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
) -> np.ndarray:
    if method == "previous":
        index = np.searchsorted(t, times, side="right") - 1
        if defined.all():
            return values[index]  # every row has a value: keep the column's dtype
        out = np.full(len(times), np.nan)
        out[defined] = values[index[defined]]
        return out
    out = np.full(len(times), np.nan)
    if defined.any():
        out[defined] = np.interp(times[defined], t, values.astype(np.float64))
    return out


class Slice:
    """The same interval of device time from several streams.

    Index by stream path, such as `s["/0/vector"]` or `s["/imu"]`, or by
    stream name when only one stream has it: `s["vector"]`. Each stream keeps
    its own rate and its own `time` array, unless `resample()` puts them on
    one stream's times. `to_pandas()` converts it.
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
        at every remaining time. "previous" keeps a column's dtype when no
        value is NaN. Every stream keeps its source `rate_hz`, and its
        `resampled` attribute names the method.
        """
        if edges not in ("nan", "trim"):
            raise ValueError("edges must be 'nan' or 'trim'")
        _ = self.timebase  # raises when the streams mix timebases
        reference = self[on]
        others = [stream for stream in self if stream.path != reference.path]
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
        for stream in self:
            if stream.path == reference.path:
                columns = {
                    name: values[keep] for name, values in stream._columns.items()
                }
                streams.append(stream._derived(times, columns, None))
                continue
            m = methods[stream.path]
            mask = defined[stream.path][keep]
            columns = {
                name: _resample_column(stream.time, values, times, m, mask)
                for name, values in stream._columns.items()
            }
            streams.append(stream._derived(times, columns, m))
        return Slice(streams, self._host_offsets, aligned_on=reference.path)

    def to_pandas(self, *, long: bool = False) -> Any:
        """The slice as pandas. Needs pandas.

        A resampled slice becomes one DataFrame indexed by `time`, with a
        column for each (stream, column). Any other slice becomes a dict of
        stream path to DataFrame, as its streams have different times.
        `long=True` returns one DataFrame with a row per value, with columns
        `time`, `stream`, `column` and `value`, for any slice.
        """
        pd = _pandas()
        if long:
            frames = [
                pd.DataFrame(
                    {
                        "time": stream.time,
                        "stream": stream.path,
                        "column": name,
                        "value": values.astype(np.float64),
                    }
                )
                for stream in self
                for name, values in stream._columns.items()
            ]
            return pd.concat(frames, ignore_index=True)
        if self._aligned_on is None:
            return {stream.path: stream.to_pandas() for stream in self}
        data = {
            (stream.path, name): values
            for stream in self
            for name, values in stream._columns.items()
        }
        frame = pd.DataFrame(
            data, index=pd.Index(self[self._aligned_on].time, name="time")
        )
        frame.columns = pd.MultiIndex.from_tuples(
            frame.columns, names=["stream", "column"]
        )
        return frame

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
            if stream.resampled:
                rows = (
                    f"{len(stream)} rows, {stream.resampled} from {stream.rate_hz:g} Hz"
                )
            else:
                rows = f"{len(stream)} rows at {stream.rate_hz:g} Hz"
            lines.append(f"  {stream.path}: {rows}, {stream.columns}")
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
