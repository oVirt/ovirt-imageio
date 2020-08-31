# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging

from . import errors
from . import stats
from . import util

log = logging.getLogger("ops")


class EOF(Exception):
    """ Raised when no more data is available and size was not specifed """


class Canceled(Exception):
    """ Raised when operation was canceled """


class Operation:

    # Should be overriden in sub classes.
    name = "operation"

    def __init__(self, size=None, offset=0, buf=None, clock=None):
        self._size = size
        self._offset = offset
        self._buf = buf
        self._done = 0
        self._clock = clock or stats.NullClock()
        self._canceled = False

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
        return self._size - self._done

    def run(self):
        with self._clock.run(self.name) as s:
            res = self._run()
            s.bytes += self.done
        return res

    def _run(self):
        raise NotImplementedError("Must be implemented by sub class")

    def cancel(self):
        log.debug("Cancelling operation %s", self)
        self._canceled = True

    def _record(self, name):
        """
        Return context manager for recording stats.
        """
        return self._clock.run(self.name + "." + name)

    def __repr__(self):
        return ("<{self.__class__.__name__} "
                "size={self.size} "
                "offset={self._offset} "
                "done={self.done} "
                "at 0x{id}>").format(self=self, id=id(self))


class Read(Operation):
    """
    Read data source backend to file object.
    """

    name = "read"

    def __init__(self, src, dst, buf, size, offset=0, clock=None):
        super().__init__(size=size, offset=offset, buf=buf, clock=clock)
        self._src = src
        self._dst = dst

    def _run(self):
        skip = self._offset % self._src.block_size
        self._src.seek(self._offset - skip)
        if skip:
            self._read_chunk(skip)
        while self._todo:
            self._read_chunk()

    def _read_chunk(self, skip=0):
        if self._src.tell() % self._src.block_size:
            raise errors.PartialContent(self.size, self.done)

        # If self._todo is not aligned to backend block_size we read complete
        # block and drop up to block_size - 1 bytes.
        aligned_todo = util.round_up(self._todo, self._src.block_size)

        with memoryview(self._buf)[:aligned_todo] as view:
            with self._record("read") as s:
                count = self._src.readinto(view)
                s.bytes += count
            if count == 0:
                raise errors.PartialContent(self.size, self.done)

        size = min(count - skip, self._todo)
        with memoryview(self._buf)[skip:skip + size] as view:
            with self._record("write") as s:
                self._dst.write(view)
                s.bytes += size
        self._done += size

        if self._canceled:
            raise Canceled


class Write(Operation):
    """
    Write data from file object to destination backend.
    """

    name = "write"

    def __init__(self, dst, src, buf, size=None, offset=0, flush=True,
                 clock=None):
        super().__init__(size=size, offset=offset, buf=buf, clock=clock)
        self._src = src
        self._dst = dst
        self._flush = flush

    @property
    def _todo(self):
        if self._size is None:
            return len(self._buf)
        return self._size - self._done

    def _run(self):
        try:
            self._dst.seek(self._offset)

            # If offset is not aligned to block size, receive partial chunk
            # until the start of the next block.
            unaligned = self._offset % self._dst.block_size
            if unaligned:
                count = min(self._todo, self._dst.block_size - unaligned)
                self._write_chunk(count)

            # Now current file position is aligned to block size and we can
            # receive full chunks.
            while self._todo:
                count = min(self._todo, len(self._buf))
                self._write_chunk(count)
        except EOF:
            pass

        if self._flush:
            with self._record("flush"):
                self._dst.flush()

    def _write_chunk(self, count):
        self._buf.seek(0)
        with memoryview(self._buf)[:count] as view:
            read = 0
            while read < count:
                with view[read:] as v:
                    with self._record("read") as s:
                        n = self._src.readinto(v)
                        s.bytes += n
                if not n:
                    break
                read += n

            pos = 0
            while pos < read:
                with view[pos:read] as v:
                    with self._record("write") as s:
                        n = self._dst.write(v)
                        s.bytes += n
                pos += n

        self._done += read
        if read < count:
            if self._size is None:
                raise EOF
            raise errors.PartialContent(self.size, self.done)

        if self._canceled:
            raise Canceled


class Zero(Operation):
    """
    Zero byte range.
    """

    name = "zero"

    # Limit zero size so we update self._done frequently enough to provide
    # progress even with slow storage.
    #
    # Large concurrent zero requests may lead to SCSI timeouts. These errors
    # seen on LIO server emulating thin provisioning:
    #
    #   Unable to recover from DataOut timeout while in ERL=0, closing iSCSI
    #   connection
    #
    # Limiting request size seems to avoid these timeouts. The disadvantage is
    # slower zeroing with file storage, but in this case the zeroing is
    # extremely fast so the difference is tiny.
    MAX_STEP = 128 * 1024**2

    def __init__(self, dst, size, offset=0, flush=False, clock=None):
        super().__init__(size=size, offset=offset, clock=clock)
        self._dst = dst
        self._flush = flush

    def _run(self):
        self._dst.seek(self._offset)

        while self._todo:
            step = min(self._todo, self.MAX_STEP)
            with self._record("zero") as s:
                n = self._dst.zero(step)
                s.bytes += n
            self._done += n
            if self._canceled:
                raise Canceled

        if self._flush:
            with self._record("flush"):
                self._dst.flush()


class Flush(Operation):
    """
    Flush received data to storage.
    """

    name = "flush"

    def __init__(self, dst, clock=None):
        super().__init__(clock=clock)
        self._dst = dst

    def _run(self):
        self._dst.flush()
