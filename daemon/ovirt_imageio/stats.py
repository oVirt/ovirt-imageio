# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import collections
import time

from contextlib import contextmanager


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
        self._stats = collections.OrderedDict()

    def start(self, name):
        s = self._stats.get(name)
        if s is None:
            s = self._stats[name] = Stats(name)

        if s.started is not None:
            raise RuntimeError("Stats %r was already started" % name)

        s.started = time.time()
        s.ops += 1

    def stop(self, name):
        s = self._stats.get(name)
        if s is None:
            raise RuntimeError("No such stats %r" % name)

        if s.started is None:
            raise RuntimeError("Stats %r was not started" % name)

        elapsed = time.time() - s.started
        s.seconds += elapsed
        s.started = None

        return elapsed

    @contextmanager
    def run(self, name):
        self.start(name)
        try:
            yield
        finally:
            self.stop(name)

    def __repr__(self):
        now = time.time()
        stats = []
        for s in self._stats.values():
            if s.started is not None:
                seconds = now - s.started
            else:
                seconds = s.seconds
            stats.append("[%s %d ops, %.6f s]" % (s.name, s.ops, seconds))
        return " ".join(stats)


class NullClock(object):
    """
    Clock that does nothing.

    This avoids checking for None clock, so users can do:

        with clock.run("name"):
            stuff to measure...

    Even if timing is disabled.
    """

    def start(self, name):
        pass

    def stop(self, name):
        return 0

    @contextmanager
    def run(self, name):
        yield

    def __repr__(self):
        return ""


class Stats(object):

    def __init__(self, name):
        self.name = name
        self.seconds = 0.0
        self.ops = 0
        self.started = None
