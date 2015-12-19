# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from contextlib import contextmanager, closing
import fcntl
import io
import mmap
import os

from . import util
from . import errors

# This value is used by vdsm when copying image data using dd. Smaller values
# save memory, and larger values minimize syscall and python calls overhead.
BUFFERSIZE = 1024 * 1024

# Typical logical block size of the underlying storage, which should be
# sufficient for doing direct I/O.
BLOCKSIZE = 512


class Operation(object):

    def __init__(self, path, size, buffersize=BUFFERSIZE):
        self._path = path
        self._size = size
        self._buffersize = buffersize
        self._todo = size

    @property
    def size(self):
        return self._size

    @property
    def done(self):
        return self._size - self._todo


class Send(Operation):
    """
    Send data from path to file object using directio.
    """

    def __init__(self, path, dst, size, buffersize=BUFFERSIZE):
        super(Send, self).__init__(path, size, buffersize=buffersize)
        self._dst = dst

    def run(self):
        with io.FileIO(self._path, "r") as src, \
                aligned_buffer(self._buffersize) as buf:
            enable_directio(src.fileno())
            while self._todo:
                if src.tell() % BLOCKSIZE:
                    raise errors.PartialContent(self.size, self.done)
                count = util.uninterruptible(src.readinto, buf)
                if count == 0:
                    raise errors.PartialContent(self.size, self.done)
                count = min(count, self._todo)
                self._dst.write(buffer(buf, 0, count))
                self._todo -= count


class Receive(Operation):
    """
    Receive data from file object to path using directio.
    """

    def __init__(self, path, src, size, buffersize=BUFFERSIZE):
        super(Receive, self).__init__(path, size, buffersize=buffersize)
        self._src = src

    def run(self):
        with io.FileIO(self._path, "r+") as dst, \
                aligned_buffer(self._buffersize) as buf:
            enable_directio(dst.fileno())
            while self._todo:
                count = min(self._todo, self._buffersize)
                chunk = self._src.read(count)
                if len(chunk) < count:
                    raise errors.PartialContent(self.size,
                                                self.done + len(chunk))
                buf[:count] = chunk
                if count % BLOCKSIZE:
                    disable_directio(dst.fileno())
                towrite = count
                while towrite:
                    offset = count - towrite
                    wbuf = buffer(buf, offset, count)
                    towrite -= util.uninterruptible(dst.write, wbuf)
                self._todo -= count
            os.fsync(dst.fileno())


@contextmanager
def aligned_buffer(size):
    """
    Return buffer aligned to page size, which work for doing direct I/O.

    Note: we use shared map to make direct io safe if fork is invoked in
    another thread concurrently with the direct io.

    Using private maps with direct io can cause data corruption and undefind
    behavior in the parent or the child processes. This restriction does not
    apply to memory buffer created with MAP_SHARED. See open(2) for more info.
    """
    buf = mmap.mmap(-1, size, mmap.MAP_SHARED)
    with closing(buf):
        yield buf


def enable_directio(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_DIRECT)


def disable_directio(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~os.O_DIRECT)
