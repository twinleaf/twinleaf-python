#!/usr/bin/env python3
"""Read the same interval from every stream of a device tree.

    tl-sync.py [url] [seconds] [stream ...]

For a VMR behind a COMM-USB hub, for example:

    tl-sync.py serial:///dev/cu.usbmodem14201 5 /0/vector /imu /bar
"""

import sys

import twinleaf

url = sys.argv[1] if len(sys.argv) > 1 else None
seconds = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
streams = sys.argv[3:] or None

tree = twinleaf.connect(url)
print(tree)
print()

s = tree.samples(seconds, streams)
print(f"{seconds:g} s of {s.timebase}, from {s.start:.6f} to {s.end:.6f}")
for stream in s:
    print(f"{stream.path}: {len(stream)} rows at {stream.rate_hz:g} Hz")
    for column in stream.columns:
        values = stream[column]
        units = stream.units[column]
        print(f"  {column:<16} mean {values.mean():12.6g} {units}")
for gap in s.gaps:
    print(f"lost {gap.samples} samples of {gap.stream} after {gap.after:.6f}")
