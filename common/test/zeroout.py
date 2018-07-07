# ovirt-imageio
# Copyright (C) 2017-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
Test script for directio.BlockIO, simulating blkdiscard --zeroout.
"""

from __future__ import absolute_import

import argparse
import os
import stat
import time

from ovirt_imageio_common import directio


def humansize(s):
    if s.isdigit():
        return int(s)
    value, unit = s[:-1], s[-1]
    value = int(value)
    unit = unit.lower()
    if unit == "k":
        return value * 1024
    elif unit == "m":
        return value * 1024**2
    elif unit == "g":
        return value * 1024**3
    else:
        raise ValueError("Unsupported unit: %r" % unit)


parser = argparse.ArgumentParser()

parser.add_argument(
    "-l", "--length",
    dest="length",
    type=humansize,
    help=("The  number of bytes to zero (counting from the starting point) "
          "(default entire device)"))

parser.add_argument(
    "-o", "--offset",
    dest="offset",
    type=humansize,
    default=0,
    help="Byte offset into the device from which to start zeroing (default 0)")

parser.add_argument(
    "-p", "--step",
    dest="step",
    type=humansize,
    help=("The number of bytes to discard within one iteration. The default "
          "is to discard all by one ioctl call"))

parser.add_argument(
    "device",
    help="device")

args = parser.parse_args()

device_stat = os.stat(args.device)
if not stat.S_ISBLK(device_stat.st_mode):
    parser.error("Not a block device: %r" % args.device)

start_time = time.time()

with directio.open(args.device, "w") as f:
    if args.length is None:
        f.seek(0, os.SEEK_END)
        device_size = f.tell()
        args.length = device_size - args.offset

    if args.step is None:
        args.step = args.length

    f.seek(args.offset)

    count = args.length
    while count:
        step = min(args.step, count)
        f.zero(step)
        count -= step

    f.flush()

elapsed_time = time.time() - start_time

print "Zero %.2f GiB in %.2f seconds (%.2f GiB/s)" % (
    float(args.length) / 1024**3,
    elapsed_time,
    float(args.length) / 1024**3 / elapsed_time)
