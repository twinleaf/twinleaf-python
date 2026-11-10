#!/usr/bin/env python3
"""List every device behind one port, and read a setting and samples from each."""

import sys

import twinleaf

url = sys.argv[1] if len(sys.argv) > 1 else None
tree = twinleaf.connect(url)
print(tree)
print()

for device in tree:
    name = device.settings.dev.name()
    print(f"{device._route} {name}")
    for stream in device._get_metadata()["streams"]:
        rows = getattr(device.samples, stream)(n=1)
        print(f"  {stream}: {dict(zip(rows[0], rows[1]))}")
