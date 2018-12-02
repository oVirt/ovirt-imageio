# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
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
import os
import stat

from .. import ioutil
from .. import util

# Typical logical block size of the underlying storage, which should be
# sufficient for doing direct I/O.
BLOCKSIZE = 512

log = logging.getLogger("file")


def open(path, mode, direct=True, buffer_size=1024**2):
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
        fio.name = path
        mode = os.fstat(fd).st_mode
        if stat.S_ISBLK(mode):
            return BlockIO(fio)
        else:
            return FileIO(fio, buffer_size=buffer_size)
    except:
        fio.close()
        raise


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
        log.debug("Opening file backend path=%s mode=%s)",
                  fio.name, fio.mode)
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
            log.debug("Closing file backend path=%s", self._fio.name)
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

    def __init__(self, fio, buffer_size=1024**2):
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
            self._buf = util.aligned_buffer(self._buffer_size)
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
