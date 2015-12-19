#!/usr/bin/python
# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import sys
import time
from imaged import util

MB = 1024 * 1024


def progress(op, stream=sys.stderr):
    start = util.monotonic_time()

    while op.done < op.size:
        time.sleep(0.1)
        elapsed = util.monotonic_time() - start
        progress = float(op.done) / op.size * 100
        rate = op.done / elapsed / MB
        stream.write("[ %6.02f%% ] %5.02f MiB/s %5.02fs\r" %
                     (progress, rate, elapsed))
        stream.flush()

    stream.write("[   done  ]\n")
