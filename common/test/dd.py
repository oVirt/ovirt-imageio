# ovirt-imageio
# Copyright (C) 2017-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
Test script for ioutil.is_zero, simulating dd conv=sparse.
"""

from __future__ import absolute_import

import argparse
import errno
import mmap
import os
import time

from contextlib import closing

from ovirt_imageio_common import directio
from ovirt_imageio_common import ioutil


def kibibyte(s):
    return int(s) * 1024


def gigabyte(s):
    return int(s) * 1024**3


parser = argparse.ArgumentParser()

parser.add_argument(
    "-b", "--blocksize-kb",
    dest="blocksize",
    type=kibibyte,
    default=kibibyte(128),
    help="block size in KiB (defualt 128 KiB)")
parser.add_argument(
    "-s", "--size",
    dest="size",
    type=gigabyte,
    help="size to read in GiB (default filename size)")
parser.add_argument(
    "-i",
    "--direct-input",
    dest="direct_input",
    action="store_true",
    help="use direct I/O for input file (default False)")
parser.add_argument(
    "-o",
    "--direct-output",
    dest="direct_output",
    action="store_true",
    help="use direct I/O for output file (default False)")
parser.add_argument(
    "input",
    help="input filename")
parser.add_argument(
    "output",
    help="output filename")

args = parser.parse_args()

if args.size is None:
    args.size = os.path.getsize(args.input)
    if args.size == 0:
        parser.error("Cannot determine file size, please specify --size")

start = time.time()

buf = mmap.mmap(-1, args.blocksize)
with closing(buf), \
        directio.open(args.input, "r", direct=args.direct_input) as src, \
        directio.open(args.output, "w", direct=args.direct_output) as dst:
    try:
        dst.truncate(args.size)
    except EnvironmentError as e:
        if e.errno != errno.EINVAL:
            raise
    pos = 0
    while pos < args.size:
        n = src.readinto(buf)
        n = min(n, args.size - pos)
        if ioutil.is_zero(buffer(buf, 0, n)):
            dst.seek(n, os.SEEK_CUR)
        else:
            written = 0
            while written < n:
                wbuf = buffer(buf, written, n - written)
                written += dst.write(wbuf)
        pos += n

    dst.flush()

elapsed = time.time() - start

print "Copied %.2f GiB, in %.3f seconds (%.2f MiB/s)" % (
    float(pos) / 1024**3,
    elapsed,
    float(pos) / 1024**2 / elapsed)
