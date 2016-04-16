# ovirt-image-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
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

    def __init__(self, path, size, offset=0, buffersize=BUFFERSIZE):
        self._path = path
        self._size = size
        self._offset = offset
        self._buffersize = min(round_up(size), buffersize)
        self._todo = size

    @property
    def size(self):
        return self._size

    @property
    def done(self):
        return self._size - self._todo

    def __repr__(self):
        return ("<{self.__class__.__name__} path={self._path!r} "
                "size={self.size} offset={self._offset} "
                "buffersize={self._buffersize} done={self.done} "
                "at 0x{id}>").format(self=self, id=id(self))


class Send(Operation):
    """
    Send data from path to file object using directio.
    """

    def __init__(self, path, dst, size, offset=0, buffersize=BUFFERSIZE):
        super(Send, self).__init__(path, size, offset=offset,
                                   buffersize=buffersize)
        self._dst = dst

    def run(self):
        with io.FileIO(self._path, "r") as src, \
                aligned_buffer(self._buffersize) as buf:
            enable_directio(src.fileno())
            if self._offset:
                skip = self._seek_to_first_block(src)
                self._send_chunk(src, buf, skip)
            while self._todo:
                self._send_chunk(src, buf)

    def _seek_to_first_block(self, src):
        skip = self._offset % BLOCKSIZE
        src.seek(self._offset - skip)
        return skip

    def _send_chunk(self, src, buf, skip=0):
        if src.tell() % BLOCKSIZE:
            raise errors.PartialContent(self.size, self.done)
        count = util.uninterruptible(src.readinto, buf)
        if count == 0:
            raise errors.PartialContent(self.size, self.done)
        size = min(count - skip, self._todo)
        self._dst.write(buffer(buf, skip, size))
        self._todo -= size


class Receive(Operation):
    """
    Receive data from file object to path using directio.
    """

    def __init__(self, path, src, size, offset=0, buffersize=BUFFERSIZE):
        super(Receive, self).__init__(path, size, offset=offset,
                                      buffersize=buffersize)
        self._src = src

    def run(self):
        with io.FileIO(self._path, "r+") as dst, \
                aligned_buffer(self._buffersize) as buf:
            if self._offset:
                remaining = self._seek_before_first_block(dst)
                if remaining:
                    self._receive_chunk(dst, buf, remaining)
            enable_directio(dst.fileno())
            while self._todo >= self._buffersize:
                self._receive_chunk(dst, buf, self._buffersize)
            disable_directio(dst.fileno())
            self._receive_chunk(dst, buf, self._todo)
            os.fsync(dst.fileno())

    def _seek_before_first_block(self, dst):
        dst.seek(self._offset)
        reminder = self._offset % BLOCKSIZE
        if reminder:
            return min(self._todo, BLOCKSIZE - reminder)
        return 0

    def _receive_chunk(self, dst, buf, count):
        buf.seek(0)
        toread = count
        while toread:
            chunk = self._src.read(toread)
            if chunk == "":
                break
            buf.write(chunk)
            toread -= len(chunk)
        if buf.tell() < count:
            raise errors.PartialContent(self.size, self.done + buf.tell())
        towrite = count
        while towrite:
            offset = count - towrite
            size = count - offset
            wbuf = buffer(buf, offset, size)
            towrite -= util.uninterruptible(dst.write, wbuf)
        self._todo -= count


def round_up(n, size=BLOCKSIZE):
    n = n + size - 1
    return n - (n % size)


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
