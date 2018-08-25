# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import fcntl
import io
import logging
import mmap
import os
import stat

from contextlib import closing

from . import errors
from . import ioutil
from . import util

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

    def __init__(self, path, size=None, offset=0, buffersize=BUFFERSIZE,
                 clock=util.NullClock()):
        self._path = path
        self._size = size
        self._offset = offset
        if self._size:
            self._buffersize = min(round_up(size), buffersize)
        else:
            self._buffersize = buffersize
        self._done = 0
        self._active = True
        self._clock = clock
        self._clock.start("operation")

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
            self._clock.stop("operation")

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

    def __init__(self, path, dst, size=None, offset=0, buffersize=BUFFERSIZE,
                 clock=util.NullClock()):
        super(Send, self).__init__(path, size=size, offset=offset,
                                   buffersize=buffersize, clock=clock)
        self._dst = dst

    def _run(self):
        for chunk in self:
            with self._clock.run("write"):
                self._dst.write(chunk)

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

        with self._clock.run("read"):
            count = src.readinto(buf)
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
                 buffersize=BUFFERSIZE, clock=util.NullClock()):
        super(Receive, self).__init__(path, size=size, offset=offset,
                                      buffersize=buffersize, clock=clock)
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
                    with self._clock.run("sync"):
                        dst.flush()

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
            with self._clock.run("read"):
                chunk = self._src.read(toread)
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
            with self._clock.run("write"):
                written = dst.write(wbuf)
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
                 buffersize=BUFFERSIZE, clock=util.NullClock(), sparse=False):
        super(Zero, self).__init__(path, size=size, offset=offset,
                                   buffersize=buffersize, clock=clock)
        self._flush = flush
        self._sparse = sparse

    def _run(self):
        with open(self._path, "r+", buffer_size=self._buffersize) as dst:
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
        while count:
            # Use small steps so we update self._done regularly, and avoid
            # blocking in kernel for too long time. Zeroing 128 MiB take less
            # than 1 second on my poor LIO storage.
            step = min(count, 128 * 1024**2)
            with self._clock.run("zero"):
                if self._sparse:
                    dst.trim(step)
                else:
                    dst.zero(step)
            count -= step
            self._done += step

    def zero_unaligned(self, dst, offset, count):
        """
        Zero count bytes at offset from current dst position.
        """
        buf = aligned_buffer(BLOCKSIZE)
        with closing(buf):
            # 1. Read complete block from storage.
            with self._clock.run("read"):
                read = dst.readinto(buf)
            if read != BLOCKSIZE:
                raise errors.PartialContent(BLOCKSIZE, read)

            # 2. Zero count bytes in the block.
            buf[offset:offset + count] = b"\0" * count

            # 3. Write the modified block back to storage.
            dst.seek(-BLOCKSIZE, os.SEEK_CUR)
            with self._clock.run("write"):
                written = dst.write(buf)
            if written != BLOCKSIZE:
                raise errors.PartialContent(BLOCKSIZE, written)

            self._done += count

    def flush(self, dst):
        with self._clock.run("flush"):
            dst.flush()


class Flush(Operation):
    """
    Flush received data to storage.
    """

    def __init__(self, path, clock=util.NullClock()):
        super(Flush, self).__init__(path, clock=clock)

    def _run(self):
        with open(self._path, "r+") as dst:
            with self._clock.run("flush"):
                dst.flush()


def open(path, mode, direct=True, buffer_size=BUFFERSIZE):
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
    fio = io.FileIO(fd, mode, closefd=True)
    try:
        mode = os.fstat(fd).st_mode
        if stat.S_ISBLK(mode):
            return BlockIO(fio)
        else:
            return FileIO(fio, buffer_size=buffer_size)
    except:
        fio.close()
        raise


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


class BaseIO(object):
    """
    Abstract I/O backend.
    """

    def __init__(self, fio):
        """
        Initizlie an I/O backend.

        Arguments:
            fio (io.FileIO): underlying file object.
        """
        self._fio = fio

    # io.FileIO interface

    def readinto(self, buf):
        return util.uninterruptible(self._fio.readinto, buf)

    def write(self, buf):
        return util.uninterruptible(self._fio.write, buf)

    def tell(self):
        return self._fio.tell()

    def seek(self, pos, how=os.SEEK_SET):
        return self._fio.seek(pos, how)

    def truncate(self, size):
        util.uninterruptible(self._fio.truncate, size)

    def fileno(self):
        return self._fio.fileno()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        try:
            self.close()
        except Exception:
            # Do not hide the original error.
            if t is None:
                raise
            log.exception("Error closing")

    def close(self):
        if self._fio:
            try:
                self._fio.close()
            finally:
                self._fio = None

    # BaseIO interface.

    def zero(self, count):
        """
        Allocate and zero count bytes at current file position.
        """
        raise NotImplementedError

    def trim(self, count):
        """
        Deallocate count bytes at curent file position. After successful call,
        reading from this range will return zeroes.
        """
        raise NotImplementedError

    def flush(self):
        return os.fsync(self.fileno())


class BlockIO(BaseIO):
    """
    Block device I/O backend.
    """

    def __init__(self, fio):
        """
        Initialize a BlockIO backend.

        Arguments:
            fio (io.FileIO): underlying file object.
        """
        super(BlockIO, self).__init__(fio)
        # May be set to False if the first call to fallocate() reveal that it
        # is not supported.
        self._can_fallocate = True

    def zero(self, count):
        """
        Zero count bytes at current file position.
        """
        offset = self.tell()

        # First try fallocate(). It works also for block devices since kernel
        # 4.9. We prefer it since it also invalidates the page cache, avoiding
        # reading stale data.
        if self._can_fallocate:
            mode = ioutil.FALLOC_FL_ZERO_RANGE
            try:
                util.uninterruptible(ioutil.fallocate, self.fileno(), mode,
                                     offset, count)
            except EnvironmentError as e:
                # On RHEL 7.5 (kenerl 3.10.0) this will fail with ENODEV.
                if e.errno not in (errno.EOPNOTSUPP, errno.ENODEV):
                    raise
                # fallocate() is not supported - do not try again.
                log.debug("fallocate(mode=%r) is not supported, zeroing "
                          "using BLKZEROOUT",
                          mode)
                self._can_fallocate = False
            else:
                self.seek(offset + count)
                return

        # If we reach this, this kernel does not support fallocate() for block
        # devices, so we fallback to BLKZEROOUT.
        util.uninterruptible(
            ioutil.blkzeroout, self.fileno(), offset, count)
        self.seek(offset + count)

    # Emulate trim using zero.
    trim = zero


class FileIO(BaseIO):
    """
    File I/O backend.
    """

    def __init__(self, fio, buffer_size=BUFFERSIZE):
        """
        Initialize a FileIO backend.

        Arguments:
            fio (io.FileIO): underlying file object.
            buffer_size (int): size of buffer used in zero() if manual zeroing
                is needed.
        """
        super(FileIO, self).__init__(fio)
        # These will be set to False if the first call to fallocate() reveal
        # that it is not supported on the current file system.
        self._can_zero_range = True
        self._can_punch_hole = True
        self._can_fallocate = True
        # If we cannot use fallocate, we fallback to manual zero, using this
        # buffer.
        self._buffer_size = buffer_size
        self._buf = None

    def zero(self, count):
        offset = self.tell()

        # First try the modern way. If this works, we can zero a range using
        # single call. Unfortunately, this does not work with NFS 4.2.
        if self._can_zero_range:
            mode = ioutil.FALLOC_FL_ZERO_RANGE
            if self._fallocate(mode, offset, count):
                self.seek(offset + count)
                return
            else:
                log.debug("Cannot zero range")
                self._can_zero_range = False

        # Next try to punch a hole and then allocate the range. This hack is
        # used by qemu since 2015.
        # See https://github.com/qemu/qemu/commit/1cdc3239f1bb
        if self._can_punch_hole and self._can_fallocate:
            mode = ioutil.FALLOC_FL_PUNCH_HOLE | ioutil.FALLOC_FL_KEEP_SIZE
            if self._fallocate(mode, offset, count):
                if self._fallocate(0, offset, count):
                    self.seek(offset + count)
                    return
                else:
                    log.debug("Cannot fallocate range")
                    self._can_fallocate = False
            else:
                log.debug("Cannot punch hole")
                self._can_punch_hole = False

        # If we are writing after the end of the file, we can allocate.
        if self._can_fallocate:
            size = os.fstat(self.fileno()).st_size
            if offset >= size:
                if self._fallocate(0, offset, count):
                    self.seek(offset + count)
                    return
                else:
                    log.debug("Cannot fallocate range")
                    self._can_fallocate = False

        # We have to write zeros manually.
        self._write_zeros(count)

    def trim(self, count):
        # First try to punch a hole.
        if self._can_punch_hole:
            offset = self.tell()

            # Extend file size if needed.
            size = os.fstat(self.fileno()).st_size
            if offset + count > size:
                self.truncate(offset + count)

            # And punch a hole.
            mode = ioutil.FALLOC_FL_PUNCH_HOLE | ioutil.FALLOC_FL_KEEP_SIZE
            if self._fallocate(mode, offset, count):
                self.seek(offset + count)
                return
            else:
                log.debug("Cannot punch hole")
                self._can_punch_hole = False

        # We have to write zeros manually.
        self._write_zeros(count)

    def _fallocate(self, mode, offset, count):
        """
        Try to fallocate, returning True if the attempt was successful, or
        False if this mode is not supported. Any other error is raised.
        """
        try:
            util.uninterruptible(
                ioutil.fallocate, self.fileno(), mode, offset, count)
            return True
        except EnvironmentError as e:
            if e.errno != errno.EOPNOTSUPP:
                raise
            return False

    def _write_zeros(self, count):
        """
        Write zeros manually.
        """
        if self._buf is None:
            self._buf = aligned_buffer(self._buffer_size)
        while count:
            step = min(self._buffer_size, count)
            wbuf = buffer(self._buf, 0, step)
            count -= self.write(wbuf)

    def close(self):
        if self._buf:
            try:
                self._buf.close()
            finally:
                self._buf = None
        super(FileIO, self).close()
