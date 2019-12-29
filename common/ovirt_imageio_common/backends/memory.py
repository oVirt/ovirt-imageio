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


def open(url, mode, sparse=False, dirty=False):
    """
    Open a memory backend.

    Arguments:
        url: (url): ignored, for consistency with other backends.
        mode: (str): "r" for readonly, "w" for write only, "r+" for read write.
        sparse (bool): ignored, memory backend does not support sparseness.
        dirty (bool): ignored, memory backend does not support dirty extents.
    """
    return Backend(mode)


class Backend(object):
    """
    Memory backend for testing.
    """

    def __init__(self, mode, data=None):
        if mode not in ("r", "w", "r+"):
            raise ValueError("Unsupported mode %r" % mode)
        self._mode = mode
        self._buf = io.BytesIO(data)
        self._dirty = False

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
        if context != "zero":
            raise errors.UnsupportedOperation(
                "Backend {} does not support {} extents"
                .format(self.name, context))

        # TODO: We can detect zeroes in underlying buffer and return more
        # interesting results.
        yield image.ZeroExtent(0, self.size(), False)

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
