# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import os

from .. import errors
from . import image

log = logging.getLogger("backends.memory")


def open(url, mode="r", sparse=False, dirty=False, max_connections=8,
         **options):
    """
    Open a memory backend.

    Arguments:
        url: (url): ignored, for consistency with other backends.
        mode: (str): "r" for readonly, "w" for write only, "r+" for read write.
        sparse (bool): ignored, memory backend does not support sparseness.
        dirty (bool): ignored, memory backend does not support dirty extents.
        max_connections (int): maximum number of connections per backend
            allowed on this server. Limit backends's max_readers and
            max_writers.
        **options: ignored, memory backend does not have any options.
    """
    return Backend(mode=mode, max_connections=max_connections)


class Backend:
    """
    Memory backend for testing.
    """

    def __init__(self, mode="r", data=None, max_connections=8, extents=None):
        if mode not in ("r", "w", "r+"):
            raise ValueError("Unsupported mode %r" % mode)
        log.info("Open backend mode=%r max_connections=%r",
                 mode, max_connections)
        self._mode = mode
        self._buf = data or bytearray()
        # TODO: Make size constant so we can build the default extents here.
        self._extents = extents or {}
        self._dirty = False
        self._position = 0
        self._closed = False
        self._max_connections = max_connections

    def clone(self):
        """
        Return a new backend sharing the same backing buffer.
        """
        return self.__class__(
            mode=self._mode,
            data=self._buf,
            max_connections=self._max_connections,
            extents=self._extents)

    @property
    def max_readers(self):
        return self._max_connections

    @property
    def max_writers(self):
        # This backend supports resize when writing or zeroing, so it cannot
        # support more than one writer concurrently.
        return 1

    # io.BaseIO interface

    def readinto(self, buf):
        self._check_closed()
        if not self.readable():
            raise IOError("Unsupproted operation: read")

        length = min(len(buf), self.size() - self._position)
        buf[:length] = self._buf[self._position:self._position + length]
        self._position += length

        return length

    def write(self, buf):
        self._check_closed()
        if not self.writable():
            raise IOError("Unsupproted operation: write")

        length = len(buf)

        extend = self._position + length - self.size()
        if extend > 0:
            self._buf.extend(extend * b"\0")

        self._buf[self._position:self._position + length] = buf
        self._position += length
        self._dirty = True

        return length

    def tell(self):
        self._check_closed()
        return self._position

    def seek(self, n, how=os.SEEK_SET):
        self._check_closed()
        if how == os.SEEK_SET:
            self._position = n
        elif how == os.SEEK_CUR:
            self._position += n
        elif how == os.SEEK_END:
            self._position = self.size() + n
        return self._position

    def flush(self):
        self._check_closed()
        self._dirty = False

    def close(self):
        log.info("Close backend")
        self._closed = True

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

    # Backend interface.

    def zero(self, count):
        self._check_closed()
        if not self.writable():
            raise IOError("Unsupproted operation: zero")
        return self.write(b"\0" * count)

    @property
    def block_size(self):
        return 1

    def extents(self, context="zero"):
        self._check_closed()
        # If not configured, report single data extent.
        if not self._extents and context == "zero":
            yield image.ZeroExtent(0, self.size(), False, False)
            return

        if context not in self._extents:
            raise errors.UnsupportedOperation(
                "Backend {} does not support {} extents"
                .format(self.name, context))

        for ext in self._extents[context]:
            yield ext

    # Debugging interface

    def readable(self):
        self._check_closed()
        return self._mode in ("r", "r+")

    def writable(self):
        self._check_closed()
        return self._mode in ("w", "r+")

    @property
    def dirty(self):
        """
        Returns True if backend was modifed and needs flushing.
        """
        return self._dirty

    @property
    def sparse(self):
        return False

    @property
    def name(self):
        return "memory"

    def size(self):
        self._check_closed()
        return len(self._buf)

    def data(self):
        return self._buf[:]

    def _check_closed(self):
        if self._closed:
            # Keeping io.FileIO behaviour.
            raise ValueError("Operation on closed backend")


class ReaderFrom(Backend):

    def read_from(self, reader, length, buf):
        _copy(reader, self, length, buf)


class WriterTo(Backend):

    def write_to(self, writer, length, buf):
        _copy(self, writer, length, buf)


def _copy(reader, writer, length, buf):
    step = len(buf)
    todo = length

    while todo > step:
        reader.readinto(buf)
        writer.write(buf)
        todo -= step

    with memoryview(buf)[:todo] as view:
        reader.readinto(view)
        writer.write(view)
