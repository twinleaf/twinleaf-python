#!/usr/bin/env python3

import pprint

import twinleaf

dev = twinleaf.Device()

settings = dev.settings()
pp = pprint.PrettyPrinter(indent=1)
pp.pprint(settings)
