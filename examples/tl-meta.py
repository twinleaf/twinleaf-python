#!/usr/bin/env python3

import pprint

import twinleaf

dev = twinleaf.Device()

meta = dev._get_metadata()
pp = pprint.PrettyPrinter(indent=1)
pp.pprint(meta)
