# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
io - I/O operations on backends.
"""

import logging
import threading

from collections import deque, namedtuple
from contextlib import closing
from functools import partial

from . import util

# Limit maximum zero and copy size to spread the workload better to multiple
# workers and ensure frequent progress updates when handling large extents.
MAX_ZERO_SIZE = 128 * 1024**2
MAX_COPY_SIZE = 128 * 1024**2

# NBD hard limit.
MAX_BUFFER_SIZE = 32 * 1024**2

# TODO: Needs more testing.
BUFFER_SIZE = 4 * 1024**2
MAX_WORKERS = 4

log = logging.getLogger("io")


def copy(src, dst, dirty=False, max_workers=MAX_WORKERS,
         buffer_size=BUFFER_SIZE, zero=True, progress=None, name="copy"):

    buffer_size = min(buffer_size, MAX_BUFFER_SIZE)

    with Executor(name=name) as executor:
        # This is a bit ugly. We get src and dst backends, to keep same
        # interface as the non-concurrent version. We use src backend here to
        # iterate over image extents. We need to clone src backend max_workers
        # times, and dst backend max_workers - 1) times.

        # The first worker clones src and use dst itself.
        executor.add_worker(
            partial(Handler, src.clone, lambda: dst, buffer_size, progress))

        # The rest of the workers clone both src and dst.
        for _ in range(max_workers - 1):
            executor.add_worker(
                partial(Handler, src.clone, dst.clone, buffer_size, progress))

        if progress:
            progress.size = src.size()

        if dirty:
            # We must treat the non-dirty extents as holes. We use this more
            # only with new qcow2 image.
            context = "dirty"
            zero = False
        else:
            # Caller controls zero behavior. When writing to new file we can
            # always skip zeroes, when wrting to server we need to zero since
            # we don't know the contents of disk on the server.
            context = "zero"

        try:
            # Submit requests to executor.
            for ext in src.extents(context):
                if ext.data:
                    executor.submit(Request(COPY, ext.start, ext.length))
                elif zero:
                    executor.submit(Request(ZERO, ext.start, ext.length))
                elif progress:
                    progress.update(ext.length)
        except Closed:
            # Error will be raised when exiting the context.
            log.debug("Executor failed")


# Request ops.
ZERO = "zero"
COPY = "copy"
STOP = "stop"


class Request(namedtuple("Request", "op,start,length")):

    def __new__(cls, op, start=0, length=0):
        return tuple.__new__(cls, (op, start, length))


class Executor:

    def __init__(self, name="executor", queue_depth=32):
        self._name = name
        self._workers = []
        self._queue = Queue(queue_depth)
        self._errors = []

    # Public interface.

    def add_worker(self, handler_factory):
        name = "{}/{}".format(self._name, len(self._workers))
        w = Worker(handler_factory, self._queue, self._errors, name=name)
        self._workers.append(w)

    def submit(self, req):
        """
        Submit request to queue. Blocks if the queue is full.
        """
        for req in self._split(req):
            self._queue.put(req)

    def stop(self):
        """
        Stop the executor when pending requests are processed. Blocks until all
        workers exit, and report the first executor error.
        """
        log.debug("Stopping executor %s", self._name)
        for _ in self._workers:
            try:
                self._queue.put(Request(STOP))
            except Closed:
                break
        self._join_workers()
        if self._errors:
            raise self._errors[0]

    def abort(self):
        """
        Drops pending requests and terminate all workers. Blocks until all
        workers exit.
        """
        log.debug("Aborting executor %s", self._name)
        self._queue.close()
        self._join_workers()

    # Private.

    def _join_workers(self):
        for w in self._workers:
            w.join()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        if t is None:
            # Normal shutdown.
            self.stop()
        else:
            # Do not hide exception in user context.
            try:
                self.abort()
            except Exception:
                log.exception("Error aborting executor")

    def _split(self, req):
        """
        Spread workload on all workers by splitting large requests.
        """
        step = MAX_ZERO_SIZE if req.op == ZERO else MAX_COPY_SIZE
        start = req.start
        length = req.length

        while length > step:
            yield Request(req.op, start, step)
            start += step
            length -= step

        yield Request(req.op, start, length)


class Worker:

    def __init__(self, handler_factory, queue, errors, name="worker"):
        self._handler_factory = handler_factory
        self._queue = queue
        self._errors = errors
        self._name = name

        log.debug("Starting worker %s", name)
        self._thread = util.start_thread(self._run, name=name)

    def join(self):
        log.debug("Waiting for worker %s", self._name)
        self._thread.join()

    def _run(self):
        try:
            log.debug("Worker %s started", self._name)
            handler = self._handler_factory()
            with closing(handler):
                while True:
                    req = self._queue.get()
                    if req.op is ZERO:
                        handler.zero(req)
                    elif req.op is COPY:
                        handler.copy(req)
                    elif req.op is STOP:
                        handler.flush(req)
                        break
        except Closed:
            log.debug("Worker %s cancelled", self._name)
        except Exception as e:
            self._errors.append(e)
            self._queue.close()
            log.exception("Worker %s failed", self._name)
        else:
            log.debug("Worker %s finished", self._name)


class Handler:

    def __init__(self, src_factory, dst_factory, buffer_size=BUFFER_SIZE,
                 progress=None):
        # Connecting to backend server may fail. Don't leave open connections
        # after failures.
        self._src = src_factory()
        try:
            self._dst = dst_factory()
        except Exception:
            self._src.close()
            raise

        self._buf = bytearray(buffer_size)
        self._progress = progress

    def zero(self, req):
        # TODO: Assumes complete zero(); not compatible with file backend.
        self._dst.seek(req.start)
        self._dst.zero(req.length)
        if self._progress:
            self._progress.update(req.length)

    def copy(self, req):
        self._src.seek(req.start)
        self._dst.seek(req.start)

        if hasattr(self._dst, "read_from"):
            self._dst.read_from(self._src, req.length, self._buf)
        elif hasattr(self._src, "write_to"):
            self._src.write_to(self._dst, req.length, self._buf)
        else:
            self._generic_copy(req)

        if self._progress:
            self._progress.update(req.length)

    def flush(self, req):
        self._dst.flush()

    def close(self):
        # Error while closing the destination backend should fail the
        # operation. Error in closing source is not fatal, but we want to know
        # about it.
        try:
            self._dst.close()
        finally:
            try:
                self._src.close()
            except Exception:
                log.exception("Error closing %s", self._src)

    def _generic_copy(self, req):
        # TODO: Assumes complete readinto() and write(); not compatible with
        # file backend.
        step = len(self._buf)
        todo = req.length

        while todo > step:
            self._src.readinto(self._buf)
            self._dst.write(self._buf)
            todo -= step

        with memoryview(self._buf)[:todo] as view:
            self._src.readinto(view)
            self._dst.write(view)


class Closed(Exception):
    """
    Raised when trying to access a closed queue.
    """


class Queue:
    """
    A simple queue supporting cancellation.

    Once a queue is closed, putting items or getting items will raise a Closed
    exception. This makes it easy to cancel group of threads waiting on the
    queue.
    """

    def __init__(self, max_size):
        self._cond = threading.Condition(threading.Lock())
        self._queue = deque(maxlen=max_size)
        self._closed = False

    @property
    def closed(self):
        return self._closed

    def put(self, item):
        with self._cond:
            self._wait_while(length=self._queue.maxlen)
            self._queue.append(item)
            self._cond.notify()

    def get(self):
        with self._cond:
            self._wait_while(length=0)
            item = self._queue.popleft()
            if len(self._queue) == self._queue.maxlen - 1:
                self._cond.notify()
            return item

    def _wait_while(self, length):
        if self._closed:
            raise Closed
        while len(self._queue) == length:
            self._cond.wait()
            if self._closed:
                raise Closed

    def close(self):
        with self._cond:
            self._closed = True
            self._queue.clear()
            self._cond.notify_all()
