# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import collections
import errno
import io
import mmap
import os
import threading
import time

from contextlib import contextmanager

import six


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
        t = self._timers.get(name)
        if t is None:
            t = self._timers[name] = Timer(name)

        if t.started is not None:
            raise RuntimeError("Timer %r is running" % name)

        t.started = time.time()
        t.count += 1

    def stop(self, name):
        t = self._timers.get(name)
        if t is None:
            raise RuntimeError("No such timer %r" % name)

        if t.started is None:
            raise RuntimeError("Timer %r is not running" % name)

        elapsed = time.time() - t.started
        t.total += elapsed
        t.started = None

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
        timers = []
        for t in self._timers.values():
            if t.started is not None:
                total = now - t.started
            else:
                total = t.total
            timers.append("%s=%.6f/%d" % (t.name, total, t.count))
        return "[%s]" % ", ".join(timers)


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
        return "[]"


class Timer(object):

    def __init__(self, name):
        self.name = name
        self.total = 0.0
        self.count = 0
        self.started = None


def round_up(n, size):
    n = n + size - 1
    return n - (n % size)


def round_down(n, size):
    return n - (n % size)


def aligned_buffer(size):
    """
    Return buffer aligned to page size, which work for doing direct I/O.

    Note: we use shared map to make direct io safe if fork is invoked in
    another thread concurrently with the direct io.

    Using private maps with direct io can cause data corruption and undefined
    behavior in the parent or the child processes. This restriction does not
    apply to memory buffer created with MAP_SHARED. See open(2) for more info.
    """
    return mmap.mmap(-1, size, mmap.MAP_SHARED)


def open(path, mode, direct=True, sync=False):
    """
    Open a file for direct I/O.

    Writing or reading from the file requires an aligned buffer. Only
    readinto() can be used to read from the file.

    Arguments:
        path (str): Filesystem path
        mode (str): One of ("r", "w", "r+"). The file is always opened in
            binary mode. See io.FileIO for more info on available modes.
        direct (bool): Try to minimize cache effects of the I/O to and from
            this file (O_DIRECT).
        sync (bool): Write operations on the file will complete according to
            the requirements of synchronized I/O file integrity completion
            (O_SYNC).
    """
    if mode == "r":
        flags = os.O_RDONLY
    elif mode == "w":
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    elif mode == "r+":
        flags = os.O_RDWR
    else:
        raise ValueError("Unsupported mode %r" % mode)

    if direct:
        flags |= os.O_DIRECT

    if sync:
        flags |= os.O_SYNC

    fd = os.open(path, flags)
    return io.FileIO(fd, mode, closefd=True)


def ensure_text(s, encoding='utf-8', errors='strict'):
    """
    Coerce *s* to six.text_type.

    For Python 2:
      - `unicode` -> `unicode`
      - `str` -> `unicode`

    For Python 3:
      - `str` -> `str`
      - `bytes` -> decoded to `str`

    Copied from six library.
    """
    if isinstance(s, six.binary_type):
        return s.decode(encoding, errors)
    elif isinstance(s, six.text_type):
        return s
    else:
        raise TypeError("not expecting type '%s'" % type(s))


class UnbufferedStream(object):
    """
    Unlike regular file object, read may return any amount of bytes up to the
    requested size. This behavior is probably the result of doing one syscall
    per read, without any buffering.

    This stream will break code assuming that read(n) retruns n bytes. This
    assumption is normally true, but not all file-like objects behave in this
    way.

    This simulate libvirt stream behavior used to copy imaged directly from
    libvirt.
    https://libvirt.org/html/libvirt-libvirt-stream.html#virStreamRecv
    """

    def __init__(self, chunks):
        self.chunks = collections.deque(chunks)

    def read(self, size):
        if not self.chunks:
            return b''
        chunk = self.chunks.popleft()
        res = chunk[:size]
        chunk = chunk[size:]
        if chunk:
            self.chunks.appendleft(chunk)
        return res

    def readinto(self, buf):
        chunk = self.read(len(buf))
        buf[:len(chunk)] = chunk
        return len(chunk)
