# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import logging
import os
import stat

from contextlib import closing

from .. import compat
from .. import ioutil
from .. import util

# Typical logical block size of the underlying storage, which should be
# sufficient for doing direct I/O.
BLOCKSIZE = 512

log = logging.getLogger("file")


def open(path, mode, sparse=False, buffer_size=1024**2):
    """
    Open a file backend.

    Arguments:
        path (str): filename of underlying file.
        mode: (str): "r" for readonly, "w" for write only, "r+" for read write.
        sparse (bool): deallocate space when zeroing if possible.
        buffer_size (int): size of buffer to allocate if needed.
    """
    fio = util.open(path, mode, direct=True)
    try:
        fio.name = path
        mode = os.fstat(fio.fileno()).st_mode
        if stat.S_ISBLK(mode):
            return BlockIO(fio, sparse=sparse)
        else:
            return FileIO(fio, sparse=sparse, buffer_size=buffer_size)
    except:
        fio.close()
        raise


class BaseIO(object):
    """
    Abstract I/O backend.
    """

    def __init__(self, fio, sparse=False):
        """
        Initizlie an I/O backend.

        Arguments:
            fio (io.FileIO): underlying file object.
            sparse (bool): deallocate space when zeroing if possible.
        """
        log.debug("Opening file backend path=%s mode=%s sparse=%s)",
                  fio.name, fio.mode, sparse)
        self._fio = fio
        self._sparse = sparse
        self._dirty = False

    # io.FileIO interface

    def readinto(self, buf):
        return util.uninterruptible(self._fio.readinto, buf)

    def write(self, buf):
        self._dirty = True
        if (not self._aligned(self.tell()) or len(buf) < BLOCKSIZE):
            # The slow path.
            return self._write_unaligned(buf)
        else:
            # The fast path.
            if not self._aligned(len(buf)):
                count = util.round_down(len(buf), BLOCKSIZE)
                buf = compat.bufview(buf, 0, count)

            return util.uninterruptible(self._fio.write, buf)

    def tell(self):
        return self._fio.tell()

    def seek(self, pos, how=os.SEEK_SET):
        return self._fio.seek(pos, how)

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
        Zero up to count bytes starting at current file offset.

        If this backend is opened in sparse mode, the operation will deallocate
        space. Otherwise the operation allocates new space.
        """
        self._dirty = True
        start = self.tell()
        if (not self._aligned(start) or count < BLOCKSIZE):
            # The slow path.
            count = min(count, BLOCKSIZE - start % BLOCKSIZE)
            return self._write_unaligned(b"\0" * count)
        else:
            # The fast path.
            count = util.round_down(count, BLOCKSIZE)
            if self._sparse:
                return self._trim(count)
            else:
                return self._zero(count)

    def flush(self):
        os.fsync(self._fio.fileno())
        self._dirty = False

    # Debugging interface

    @property
    def dirty(self):
        return self._dirty

    # Private

    def _aligned(self, n):
        """
        Return True if number n is aligned to block size.
        """
        return not (n & (BLOCKSIZE - 1))

    def _write_unaligned(self, buf):
        """
        Write up to BLOCKSIZE bytes from buf into the current block.

        If position is not aligned to block size, writes only up to end of
        current block.

        Perform a read-modify-write on the current block:
        1. Read the current block
        2. copy bytes from buf into the block
        3. write the block back to storage.

        Returns:
            Number of bytes written
        """
        start = self.tell()
        offset = start % BLOCKSIZE
        count = min(len(buf), BLOCKSIZE - offset)

        log.debug("Unaligned write start=%s offset=%s count=%s",
                  start, offset, count)

        block = util.aligned_buffer(BLOCKSIZE)
        with closing(block):
            # 1. Read available bytes in current block.
            self.seek(start - offset)
            self.readinto(block)

            # 2. Write new bytes into buffer.
            block[offset:offset + count] = buf[:count]

            # 3. Write block back to storage. This aligns the file to block
            # size by padding zeros if needed.
            # TODO: When writing to file system, block size may be wrong, so we
            # need to take care of short writes.
            self.seek(start - offset)
            util.uninterruptible(self._fio.write, block)

            # 4. Update position.
            self.seek(start + count)

        return count


class BlockIO(BaseIO):
    """
    Block device I/O backend.
    """

    def __init__(self, fio, sparse=False):
        """
        Initialize a BlockIO backend.

        Arguments:
            fio (io.FileIO): underlying file object.
            sparse (bool): deallocate space when zeroing if possible.
        """
        super(BlockIO, self).__init__(fio, sparse=sparse)
        # May be set to False if the first call to fallocate() reveal that it
        # is not supported.
        self._can_fallocate = True

    def _zero(self, count):
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
                util.uninterruptible(ioutil.fallocate, self._fio.fileno(),
                                     mode, offset, count)
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
                return count

        # If we reach this, this kernel does not support fallocate() for block
        # devices, so we fallback to BLKZEROOUT.
        util.uninterruptible(
            ioutil.blkzeroout, self._fio.fileno(), offset, count)
        self.seek(offset + count)
        return count

    # Emulate trim using zero.
    _trim = _zero


class FileIO(BaseIO):
    """
    File I/O backend.
    """

    def __init__(self, fio, sparse=False, buffer_size=1024**2):
        """
        Initialize a FileIO backend.

        Arguments:
            fio (io.FileIO): underlying file object.
            sparse (bool): deallocate space when zeroing if possible.
            buffer_size (int): size of buffer used in zero() if manual zeroing
                is needed.
        """
        super(FileIO, self).__init__(fio, sparse=sparse)
        # These will be set to False if the first call to fallocate() reveal
        # that it is not supported on the current file system.
        self._can_zero_range = True
        self._can_punch_hole = True
        self._can_fallocate = True
        # If we cannot use fallocate, we fallback to manual zero, using this
        # buffer.
        self._buffer_size = buffer_size
        self._buf = None

    def _zero(self, count):
        offset = self.tell()

        # First try the modern way. If this works, we can zero a range using
        # single call. Unfortunately, this does not work with NFS 4.2.
        if self._can_zero_range:
            mode = ioutil.FALLOC_FL_ZERO_RANGE
            if self._fallocate(mode, offset, count):
                self.seek(offset + count)
                return count
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
                    return count
                else:
                    log.debug("Cannot fallocate range")
                    self._can_fallocate = False
            else:
                log.debug("Cannot punch hole")
                self._can_punch_hole = False

        # If we are writing after the end of the file, we can allocate.
        if self._can_fallocate:
            size = os.fstat(self._fio.fileno()).st_size
            if offset >= size:
                if self._fallocate(0, offset, count):
                    self.seek(offset + count)
                    return count
                else:
                    log.debug("Cannot fallocate range")
                    self._can_fallocate = False

        # We have to write zeros manually.
        self._write_zeros(count)
        return count

    def _trim(self, count):
        # First try to punch a hole.
        if self._can_punch_hole:
            offset = self.tell()

            # Extend file size if needed.
            size = os.fstat(self._fio.fileno()).st_size
            if offset + count > size:
                self._truncate(offset + count)

            # And punch a hole.
            mode = ioutil.FALLOC_FL_PUNCH_HOLE | ioutil.FALLOC_FL_KEEP_SIZE
            if self._fallocate(mode, offset, count):
                self.seek(offset + count)
                return count
            else:
                log.debug("Cannot punch hole")
                self._can_punch_hole = False

        # We have to write zeros manually.
        self._write_zeros(count)
        return count

    def _fallocate(self, mode, offset, count):
        """
        Try to fallocate, returning True if the attempt was successful, or
        False if this mode is not supported. Any other error is raised.
        """
        try:
            util.uninterruptible(
                ioutil.fallocate, self._fio.fileno(), mode, offset, count)
            return True
        except EnvironmentError as e:
            if e.errno != errno.EOPNOTSUPP:
                raise
            return False

    def _truncate(self, size):
        util.uninterruptible(self._fio.truncate, size)

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
