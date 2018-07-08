# ovirt-imageio
# Copyright (C) 2017-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
Test script for directio.BlockIO() and directio.FileIO(), simulating fallocate
and blkdiscard --zeroout.
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
          "(default entire device or file)"))

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
    help=("The number of bytes to zero within one iteration. The default "
          "is to discard all by one ioctl call"))

parser.add_argument(
    "-s", "--sparse",
    dest="sparse",
    action="store_true",
    help="Deallocate zeroed space (punch holes)")

parser.add_argument(
    "filename",
    help="file or block device to fill with zeros")

args = parser.parse_args()

start_time = time.time()

with directio.open(args.filename, "r+") as f:
    if args.length is None:
        st = os.stat(args.filename)
        if stat.S_ISBLK(st.st_mode):
            f.seek(0, os.SEEK_END)
            size = f.tell()
        else:
            size = st.st_size
        args.length = size - args.offset

    if args.step is None:
        args.step = args.length

    f.seek(args.offset)

    count = args.length
    while count:
        step = min(args.step, count)
        if args.sparse:
            f.trim(step)
        else:
            f.zero(step)
        count -= step

    f.flush()

elapsed_time = time.time() - start_time

print "Zero %.2f GiB in %.3f seconds (%.2f GiB/s)" % (
    float(args.length) / 1024**3,
    elapsed_time,
    float(args.length) / 1024**3 / elapsed_time)
