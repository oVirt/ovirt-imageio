# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import fcntl
import io
import logging
import mmap
import os

from contextlib import closing

from . import util
from . import errors

# This value is used by vdsm when copying image data using dd. Smaller values
# save memory, and larger values minimize syscall and python calls overhead.
BUFFERSIZE = 1024 * 1024

# Typical logical block size of the underlying storage, which should be
# sufficient for doing direct I/O.
BLOCKSIZE = 512

log = logging.getLogger("directio")


class EOF(Exception):
    """ Raised when no more data is available and size was not specifed """


class Operation(object):

    def __init__(self, path, size=None, offset=0, buffersize=BUFFERSIZE):
        self._path = path
        self._size = size
        self._offset = offset
        if self._size:
            self._buffersize = min(round_up(size), buffersize)
        else:
            self._buffersize = buffersize
        self._done = 0
        self._active = True
        self._clock = util.Clock()
        self._clock.start("total")

    @property
    def size(self):
        return self._size

    @property
    def offset(self):
        return self._offset

    @property
    def done(self):
        return self._done

    @property
    def _todo(self):
        if self._size is None:
            return self._buffersize
        return self._size - self._done

    @property
    def active(self):
        return self._active

    def run(self):
        try:
            self._run()
        finally:
            self.close()

    def close(self):
        if self._active:
            self._active = False
            self._clock.stop("total")
            log.info("Operation stats: %s", self._clock)

    def __repr__(self):
        return ("<{self.__class__.__name__} path={self._path!r} "
                "size={self.size} offset={self._offset} "
                "buffersize={self._buffersize} done={self.done}{active} "
                "at 0x{id}>").format(
                    self=self,
                    id=id(self),
                    active=" active" if self.active else ""
                )


class Send(Operation):
    """
    Send data from path to file object using directio.
    """

    def __init__(self, path, dst, size=None, offset=0, buffersize=BUFFERSIZE):
        super(Send, self).__init__(path, size=size, offset=offset,
                                   buffersize=buffersize)
        self._dst = dst

    def _run(self):
        for chunk in self:
            self._clock.start("write")
            self._dst.write(chunk)
            elapsed = self._clock.stop("write")
            log.debug("Wrote %d bytes in %.3f seconds", len(chunk), elapsed)

    def __iter__(self):
        with open(self._path, "r") as src, \
                closing(aligned_buffer(self._buffersize)) as buf:
            try:
                if self._offset:
                    skip = self._seek_to_first_block(src)
                    yield self._next_chunk(src, buf, skip)
                while self._todo:
                    yield self._next_chunk(src, buf)
            except EOF:
                pass

    def _seek_to_first_block(self, src):
        skip = self._offset % BLOCKSIZE
        src.seek(self._offset - skip)
        return skip

    def _next_chunk(self, src, buf, skip=0):
        if src.tell() % BLOCKSIZE:
            if self._size is None:
                raise EOF
            raise errors.PartialContent(self.size, self.done)

        self._clock.start("read")
        count = util.uninterruptible(src.readinto, buf)
        elapsed = self._clock.stop("read")
        log.debug("Read %d bytes in %.3f seconds", count, elapsed)
        if count == 0:
            if self._size is None:
                raise EOF
            raise errors.PartialContent(self.size, self.done)

        size = min(count - skip, self._todo)
        self._done += size
        return buffer(buf, skip, size)


class Receive(Operation):
    """
    Receive data from file object to path using directio.
    """

    def __init__(self, path, src, size=None, offset=0, flush=True,
                 buffersize=BUFFERSIZE):
        super(Receive, self).__init__(path, size=size, offset=offset,
                                      buffersize=buffersize)
        self._src = src
        self._flush = flush

    def _run(self):
        with open(self._path, "r+") as dst, \
                closing(aligned_buffer(self._buffersize)) as buf:
            try:
                if self._offset:
                    remaining = self._seek_before_first_block(dst)
                    if remaining:
                        disable_directio(dst.fileno())
                        self._receive_chunk(dst, buf, remaining)
                        enable_directio(dst.fileno())
                while self._todo:
                    count = min(self._todo, self._buffersize)
                    self._receive_chunk(dst, buf, count)
            except EOF:
                pass
            finally:
                if self._flush:
                    self._clock.start("sync")
                    os.fsync(dst.fileno())
                    self._clock.stop("sync")

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
            self._clock.start("read")
            chunk = self._src.read(toread)
            elapsed = self._clock.stop("read")
            log.debug("Read %d bytes in %.3f seconds", len(chunk), elapsed)
            if chunk == "":
                break
            buf.write(chunk)
            toread -= len(chunk)

        towrite = buf.tell()
        while towrite:
            offset = buf.tell() - towrite
            size = buf.tell() - offset
            wbuf = buffer(buf, offset, size)
            if size % BLOCKSIZE:
                disable_directio(dst.fileno())
            self._clock.start("write")
            written = util.uninterruptible(dst.write, wbuf)
            elapsed = self._clock.stop("write")
            log.debug("Wrote %d bytes in %.3f seconds", written, elapsed)
            towrite -= written

        self._done += buf.tell()
        if buf.tell() < count:
            if self._size is None:
                raise EOF
            raise errors.PartialContent(self.size, self.done)


class Zero(Operation):
    """
    Zero byte range.

    TODO: Use more efficient backend specific method if available.
    """

    def __init__(self, path, size, offset=0, flush=False,
                 buffersize=BUFFERSIZE):
        super(Zero, self).__init__(path, size=size, offset=offset,
                                   buffersize=buffersize)
        self._flush = flush

    def _run(self):
        with open(self._path, "r+") as dst:
            # Handle offset if specified.
            if self._offset:
                reminder = self._offset % BLOCKSIZE
                if reminder:
                    # Zero the end or middle of first block (unlikely).
                    dst.seek(self._offset - reminder)
                    count = min(self._size, BLOCKSIZE - reminder)
                    self.zero_unaligned(dst, reminder, count)
                else:
                    # Offset is aligned (likely).
                    dst.seek(self._offset)

            # Zero complete blocks (likely).
            count = round_down(self._todo)
            if count:
                self.zero_aligned(dst, count)

            # Zero the start of last block if needed (unlikely).
            if self._todo:
                self.zero_unaligned(dst, 0, self._todo)

            if self._flush:
                self.flush(dst)

    def zero_aligned(self, dst, count):
        """
        Zero count bytes at current file position.
        """
        buf = aligned_buffer(self._buffersize)
        with closing(buf):
            while count:
                wbuf = buffer(buf, 0, min(self._buffersize, count))
                self._clock.start("write")
                written = util.uninterruptible(dst.write, wbuf)
                elapsed = self._clock.stop("write")
                log.debug("Wrote %d bytes in %.3f seconds", written, elapsed)
                count -= written
                self._done += written

    def zero_unaligned(self, dst, offset, count):
        """
        Zero count bytes at offset from current dst position.
        """
        buf = aligned_buffer(BLOCKSIZE)
        with closing(buf):
            # 1. Read complete block from storage.
            self._clock.start("read")
            read = util.uninterruptible(dst.readinto, buf)
            elapsed = self._clock.stop("read")
            log.debug("Read %d bytes in %.3f seconds", read, elapsed)
            if read != BLOCKSIZE:
                raise errors.PartialContent(BLOCKSIZE, read)

            # 2. Zero count bytes in the block.
            buf[offset:offset + count] = b"\0" * count

            # 3. Write the modified block back to storage.
            dst.seek(-BLOCKSIZE, os.SEEK_CUR)
            self._clock.start("write")
            written = util.uninterruptible(dst.write, buf)
            elapsed = self._clock.stop("write")
            log.debug("Wrote %d bytes in %.3f seconds", written, elapsed)
            if written != BLOCKSIZE:
                raise errors.PartialContent(BLOCKSIZE, written)

            self._done += count

    def flush(self, dst):
        self._clock.start("sync")
        os.fsync(dst.fileno())
        self._clock.stop("sync")


class Flush(Operation):
    """
    Flush received data to storage.
    """

    def __init__(self, path):
        super(Flush, self).__init__(path)

    def _run(self):
        with open(self._path, "r+") as dst:
            self._clock.start("sync")
            os.fsync(dst.fileno())
            self._clock.stop("sync")


def open(path, mode, direct=True):
    """
    Open a file for direct I/O.

    Writing or reading from the file requires an aligned buffer. Only
    readinto() can be used to read from the file.

    If direct is False, open the file for buffered I/O. You can enable direct
    I/O later using enable_directio().
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

    fd = os.open(path, flags)
    return io.FileIO(fd, mode, closefd=True)


def round_up(n, size=BLOCKSIZE):
    n = n + size - 1
    return n - (n % size)


def round_down(n, size=BLOCKSIZE):
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


def enable_directio(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_DIRECT)


def disable_directio(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~os.O_DIRECT)
