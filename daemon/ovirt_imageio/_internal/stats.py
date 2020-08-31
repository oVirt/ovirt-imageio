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

from . import util


class Clock:
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

    def __init__(self, now=time.monotonic):
        # Keep insertion order for nicer output in __repr__.
        self._stats = collections.OrderedDict()
        self._now = now

    def start(self, name):
        s = self._stats.get(name)
        if s is None:
            s = self._stats[name] = Stats(name)

        if s.started is not None:
            raise RuntimeError("Stats %r was already started" % name)

        s.started = self._now()

        return s

    def stop(self, name):
        s = self._lookup_started(name)
        return self._stop(s, True)

    def abort(self, name):
        s = self._lookup_started(name)
        return self._stop(s, False)

    @contextmanager
    def run(self, name):
        s = self.start(name)
        try:
            yield s
        except BaseException:
            self._stop(s, False)
            raise
        else:
            self._stop(s, True)

    def _lookup_started(self, name):
        s = self._stats.get(name)
        if s is None:
            raise RuntimeError("No such stats %r" % name)

        if s.started is None:
            raise RuntimeError("Stats %r was not started" % name)

        return s

    def _stop(self, s, completed):
        elapsed = self._now() - s.started
        s.seconds += elapsed
        s.started = None
        if completed:
            s.ops += 1

        return elapsed

    def __repr__(self):
        now = self._now()
        stats = []
        for s in self._stats.values():
            if s.started is not None:
                seconds = now - s.started
            else:
                seconds = s.seconds
            values = [
                "{} ops".format(s.ops),
                "{:.6f} s".format(seconds),
            ]
            if s.bytes:
                values.append(util.humansize(s.bytes))
                values.append(util.humansize(s.bytes / seconds) + "/s")

            stats.append("[{} {}]".format(s.name, ", ".join(values)))

        return " ".join(stats)


class NullClock:
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
        yield _NULL_STATS

    def __repr__(self):
        return ""


class Stats:

    def __init__(self, name):
        self.name = name
        self.seconds = 0.0
        self.ops = 0
        self.bytes = 0
        self.started = None


class NullStats:

    @property
    def bytes(self):
        return 0

    @bytes.setter
    def bytes(self, value):
        pass


_NULL_STATS = NullStats()
