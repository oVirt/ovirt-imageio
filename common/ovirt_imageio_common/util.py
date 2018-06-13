# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import collections
import errno
import os
import threading
import time


def uninterruptible(func, *args):
    while True:
        try:
            return func(*args)
        except EnvironmentError as e:
            if e.errno != errno.EINTR:
                raise


def start_thread(func, args=(), kwargs=None, name=None, daemon=True):
    if kwargs is None:
        kwargs = {}
    t = threading.Thread(target=func, args=args, kwargs=kwargs, name=name)
    t.daemon = daemon
    t.start()
    return t


def monotonic_time():
    return os.times()[4]


class Clock(object):
    """
    Measure time for complex flows.

    This clock is useful for timing complex flows, when you want to record
    multiple timings for a single flow. For example, the total time, and the
    time of each step in the flow.

    This is similar to MoinMoin.util.clock.Clock:
    https://bitbucket.org/thomaswaldmann/moin-2.0/src/

    And vdsm.common.time.Clock:
    https://github.com/oVirt/vdsm/blob/master/lib/vdsm/common/time.py#L45

    Usage::

        clock = time.Clock()
        ...
        clock.start("total")
        ...
        clock.start("read")
        clock.stop("read")
        clock.start("write")
        clock.stop("write")
        clock.start("read")
        clock.stop("read")
        clock.start("write")
        clock.stop("write")
        ...
        clock.start("sync")
        clock.stop("sync")
        ...
        clock.stop("total")
        log.info("times=%s", clock)

    """

    def __init__(self):
        # Keep insertion order for nicer output in __repr__.
        self._timers = collections.OrderedDict()

    def start(self, name):
        total, started = self._timers.get(name, (0, None))
        if started is not None:
            raise RuntimeError("Timer %r is running" % name)
        self._timers[name] = (total, time.time())

    def stop(self, name):
        try:
            total, started = self._timers[name]
        except KeyError:
            raise RuntimeError("No such timer %r" % name)
        if started is None:
            raise RuntimeError("Timer %r is not running" % name)
        elapsed = time.time() - started
        self._timers[name] = (total + elapsed, None)
        return elapsed

    def __repr__(self):
        now = time.time()
        timers = []
        for name, (total, started) in self._timers.items():
            if started is not None:
                running = "*"
                total += now - started
            else:
                running = ""
            timers.append("%s=%.6f%s" % (name, total, running))
        return "<Clock(%s)>" % ", ".join(timers)
