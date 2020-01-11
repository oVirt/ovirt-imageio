# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
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


class Backend(object):
    """
    Memory backend for testing.
    """

    def __init__(self, mode="r", data=None, max_connections=8, extents=None):
        if mode not in ("r", "w", "r+"):
            raise ValueError("Unsupported mode %r" % mode)
        log.info("Open backend mode=%r max_connections=%r",
                 mode, max_connections)
        self._mode = mode
        self._buf = io.BytesIO(data)
        # TODO: Make size constant so we can build the default extents here.
        self._extents = extents or {}
        self._dirty = False
        self._max_connections = max_connections

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
        if not self.readable():
            raise IOError("Unsupproted operation: read")
        return self._buf.readinto(buf)

    def write(self, buf):
        if not self.writable():
            raise IOError("Unsupproted operation: write")
        self._dirty = True
        return self._buf.write(buf)

    def tell(self):
        return self._buf.tell()

    def seek(self, pos, how=os.SEEK_SET):
        return self._buf.seek(pos, how)

    def flush(self):
        self._buf.flush()
        self._dirty = False

    def close(self):
        log.info("Close backend")
        self._buf.close()

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
        if not self.writable():
            raise IOError("Unsupproted operation: truncate")
        self._dirty = True
        self._buf.write(b"\0" * count)
        return count

    @property
    def block_size(self):
        return 1

    def extents(self, context="zero"):
        # If not configured, report single data extent.
        if not self._extents and context == "zero":
            yield image.ZeroExtent(0, self.size(), False)
            return

        if context not in self._extents:
            raise errors.UnsupportedOperation(
                "Backend {} does not support {} extents"
                .format(self.name, context))

        for ext in self._extents[context]:
            yield ext

    # Debugging interface

    def readable(self):
        return self._mode in ("r", "r+")

    def writable(self):
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
        old_pos = self._buf.tell()
        self._buf.seek(0, os.SEEK_END)
        result = self._buf.tell()
        self._buf.seek(old_pos, os.SEEK_SET)
        return result

    def data(self):
        return self._buf.getvalue()


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
