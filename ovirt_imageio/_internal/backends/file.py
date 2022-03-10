# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import logging
import os
import stat

from contextlib import closing

from .. import errors
from .. import extent
from .. import ioutil
from .. import util

from . common import CLOSED

log = logging.getLogger("backends.file")


def open(url, mode="r", sparse=False, dirty=False, max_connections=8,
         **options):
    """
    Open a file backend.

    Arguments:
        url (url): parsed file url of underlying file.
        mode: (str): "r" for readonly, "r+" for read write.
        sparse (bool): deallocate space when zeroing if possible.
        dirty (bool): ignored, file backend does not support dirty extents.
        max_connections (int): maximum number of connections per backend
            allowed on this server. Limit backends's max_readers and
            max_writers.
        **options: ignored, file backend does not have any options.
    """
    fio = util.open(url.path, mode, direct=True)
    try:
        fio.name = url.path
        mode = os.fstat(fio.fileno()).st_mode
        backend = BlockBackend if stat.S_ISBLK(mode) else FileBackend
        return backend(fio, sparse=sparse, max_connections=max_connections)
    except:  # noqa: E722
        fio.close()
        raise


class Backend:
    """
    Base class for file backends.
    """

    def __init__(self, fio, sparse=False, max_connections=8):
        """
        Initizlie an I/O backend.

        Arguments:
            fio (io.FileIO): underlying file object.
            sparse (bool): deallocate space when zeroing if possible.
        """
        log.debug("Open path=%r mode=%r sparse=%r max_connections=%r",
                  fio.name, fio.mode, sparse, max_connections)
        self._fio = fio
        self._sparse = sparse
        self._dirty = False
        self._max_connections = max_connections

    @property
    def max_readers(self):
        return self._max_connections

    # io.FileIO interface

    def readinto(self, buf):
        return self._fio.readinto(buf)

    def write(self, buf):
        self._dirty = True
        if (not self._aligned(self.tell()) or len(buf) < self._block_size):
            # The slow path.
            return self._write_unaligned(buf)
        else:
            # The fast path.
            if self._aligned(len(buf)):
                return self._fio.write(buf)
            else:
                count = util.round_down(len(buf), self._block_size)
                with memoryview(buf)[:count] as view:
                    return self._fio.write(view)

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
        if self._fio is not CLOSED:
            log.debug("Close path=%r dirty=%r",
                      self._fio.name, self._dirty)
            try:
                self._fio.close()
            finally:
                self._fio = CLOSED

    # Backend interface.

    def zero(self, count):
        """
        Zero up to count bytes starting at current file offset.

        If this backend is opened in sparse mode, the operation will deallocate
        space. Otherwise the operation allocates new space.
        """
        self._dirty = True
        start = self.tell()
        if (not self._aligned(start) or count < self._block_size):
            # The slow path.
            count = min(count, self._block_size - start % self._block_size)
            return self._write_unaligned(b"\0" * count)
        else:
            # The fast path.
            count = util.round_down(count, self._block_size)
            if self._sparse:
                return self._zero_sparse(count)
            else:
                return self._zero(count)

    def flush(self):
        os.fsync(self._fio.fileno())
        self._dirty = False

    @property
    def block_size(self):
        return self._block_size

    def extents(self, context="zero"):
        if context != "zero":
            raise errors.UnsupportedOperation(
                "Backend {} does not support {} extents"
                .format(self.name, context))

        # TODO: Use qemu-img map to get extents.
        yield extent.ZeroExtent(0, self.size(), False, False)

    # Debugging interface

    def readable(self):
        return self._fio.readable()

    def writable(self):
        return self._fio.writable()

    @property
    def dirty(self):
        return self._dirty

    @property
    def sparse(self):
        return self._sparse

    @property
    def name(self):
        return "file"

    def size(self):
        old_pos = self._fio.tell()
        self._fio.seek(0, os.SEEK_END)
        result = self._fio.tell()
        self._fio.seek(old_pos, os.SEEK_SET)
        return result

    # Private

    def _aligned(self, n):
        """
        Return True if number n is aligned to block size.
        """
        return not (n & (self._block_size - 1))

    def _write_unaligned(self, buf):
        """
        Write up to block_size bytes from buf into the current block.

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
        offset = start % self._block_size
        count = min(len(buf), self._block_size - offset)

        log.debug("Unaligned write start=%s offset=%s count=%s",
                  start, offset, count)

        block = util.aligned_buffer(self._block_size)
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
            self._fio.write(block)

            # 4. Update position.
            self.seek(start + count)

        return count

    def _clone(self):
        mode = self._fio.mode.replace("b", "")
        fio = util.open(self._fio.name, mode=mode, direct=True)
        try:
            return self.__class__(
                fio,
                sparse=self._sparse,
                max_connections=self._max_connections,
                block_size=self._block_size)
        except:  # noqa: E722
            fio.close()
            raise


class BlockBackend(Backend):
    """
    Block device backend.
    """

    def __init__(self, fio, sparse=False, max_connections=8, block_size=512):
        """
        Initialize a BlockBackend.

        Arguments:
            fio (io.FileIO): underlying file object.
            sparse (bool): deallocate space when zeroing if possible.
            max_connections (int): maximum number of connections per backend
                allowed on this server. Limit backends's max_readers and
                max_writers.
            block_size (int): If set, use the specified block size. Otherwise
                the value is detected automatically.
        """
        super().__init__(fio, sparse=sparse, max_connections=max_connections)
        # May be set to False if the first call to fallocate() reveal that it
        # is not supported.
        self._can_fallocate = True
        # TODO: get block size using ioctl(BLKSSZGET).
        self._block_size = block_size

    def clone(self):
        """
        Return a new backend sharing the same block device.
        """
        backend = self._clone()
        backend._can_fallocate = self._can_fallocate
        return backend

    @property
    def max_writers(self):
        return self._max_connections

    def _zero(self, count):
        """
        Zero count bytes at current file position, allocating space.
        """
        offset = self.tell()

        # First try fallocate(). It works also for block devices since kernel
        # 4.9. We prefer it since it also invalidates the page cache, avoiding
        # reading stale data.
        if self._can_fallocate:
            mode = ioutil.FALLOC_FL_ZERO_RANGE
            try:
                ioutil.fallocate(self._fio.fileno(), mode, offset, count)
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
        ioutil.blkzeroout(self._fio.fileno(), offset, count)
        self.seek(offset + count)
        return count

    # Emulate with zero.
    # TODO: With recent kernel we can use fallocate(PUNCH_HOLE). It works for
    # devices whitelisted to zero discarded range.
    _zero_sparse = _zero


class FileBackend(Backend):
    """
    Regular file backend.
    """

    def __init__(self, fio, sparse=False, max_connections=8, block_size=None):
        """
        Initialize a FileBackend.

        Arguments:
            fio (io.FileIO): underlying file object.
            sparse (bool): deallocate space when zeroing if possible.
            max_connections (int): maximum number of connections per backend
                allowed on this server. Limit backends's max_readers.
            block_size (int): If set, use the specified block size. Otherwise
                the value is detected automatically.
        """
        super().__init__(fio, sparse=sparse, max_connections=max_connections)
        # These will be set to False if the first call to fallocate() reveal
        # that it is not supported on the current file system.
        self._can_zero_range = True
        self._can_punch_hole = True
        self._can_fallocate = True
        self._block_size = block_size or self._detect_block_size()

    def clone(self):
        """
        Return a new backend sharing the same file.
        """
        backend = self._clone()
        backend._can_zero_range = self._can_zero_range
        backend._can_punch_hole = self._can_punch_hole
        backend._can_fallocate = self._can_fallocate
        return backend

    @property
    def max_writers(self):
        # Zeroing and trimming qcow2 format grows the file and assumes a single
        # writer. User that wants best performance should use the nbd backend.
        return 1

    def _detect_block_size(self):
        """
        Detect the unserlying storage block size by checking the minimal block
        size that works for direct I/O.

        There are 2 cases when we cannot detect the block size:
        - Reading from unallocated file in local XFS or Gluster over XFS. This
          should not happen now since qemu-img create always allocate the first
          block to mitigate this issue.
        - NFS, since O_DIRECT s not passed to the server

        When we cannot detect the block size we fallback to 4096.
        """
        for block_size in (1, 512, 4096):
            log.debug("Trying block size %s", block_size)
            buf = util.aligned_buffer(block_size)
            with closing(buf):
                self.seek(0)
                try:
                    self.readinto(buf)
                except EnvironmentError as e:
                    if e.errno != errno.EINVAL:
                        raise
                    continue

            self.seek(0)

            if block_size == 1:
                log.debug("Cannot detect block size - using 4096")
                block_size = 4096
            else:
                log.debug("Detected block size %s", block_size)

            return block_size

        raise RuntimeError(
            "Cannot use direct I/O with {}".format(self._fio.path))

    def _zero(self, count):
        """
        Zero count bytes at current file position, allocating space.
        """
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

    def _zero_sparse(self, count):
        """
        Zero count bytes at current file position, punching a hole.
        """
        # First try to punch a hole.
        if self._can_punch_hole:
            offset = self.tell()

            # Extend file size if needed.
            size = os.fstat(self._fio.fileno()).st_size
            if offset + count > size:
                self._truncate(offset + count)

                # If we zero the end of the file punching a hole is not needed.
                if size == offset:
                    self.seek(offset + count)
                    return count

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
            ioutil.fallocate(self._fio.fileno(), mode, offset, count)
            return True
        except EnvironmentError as e:
            if e.errno != errno.EOPNOTSUPP:
                raise
            return False

    def _truncate(self, size):
        self._fio.truncate(size)

    def _write_zeros(self, count):
        """
        Write zeros manually.
        """
        buf_size = min(count, 1024**2)
        with util.aligned_buffer(buf_size) as buf, memoryview(buf) as view:
            while count:
                count -= self.write(view[:count])
