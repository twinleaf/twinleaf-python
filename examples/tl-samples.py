#!/usr/bin/env python3

import twinleaf
import pprint

dev = twinleaf.Device()

samples_dict_getter = dev.samples # All samples
samples_list_getter = dev.samples.imu.imu.accel # Wildcard samples
#samples_list_getter = dev.samples.imu.imu.accel.x # Specific column

samples_dict = samples_dict_getter(n=10)
for _id, stream in samples_dict.items():
    for column, values in stream.items():
        print(f"{column}: {values}")
    print()
print()

samples_list = samples_list_getter(n=10)
for sample in samples_list:
    for column in sample:
        print(f"{column:<20}", end='')
    print()
