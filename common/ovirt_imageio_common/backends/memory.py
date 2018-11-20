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

log = logging.getLogger("memory")


def open(mode):
    if mode not in ("r", "w", "r+"):
        raise ValueError("Unsupported mode %r" % mode)
    return Backend(mode)


class Backend(object):
    """
    Memory backend for testing.
    """

    def __init__(self, mode):
        self._mode = mode
        self._buf = io.BytesIO()

    # io.BaseIO interface

    def readinto(self, buf):
        if not self._readable():
            raise IOError("Unsupproted operation: read")
        return self._buf.readinto(buf)

    def write(self, buf):
        if not self._writable():
            raise IOError("Unsupproted operation: write")
        return self._buf.write(buf)

    def tell(self):
        return self._buf.tell()

    def seek(self, pos, how=os.SEEK_SET):
        return self._buf.seek(pos, how)

    def truncate(self, size):
        if not self._writable():
            raise IOError("Unsupproted operation: truncate")
        self._buf.truncate(size)

    def fileno(self):
        return self._buf.fileno()

    def flush(self):
        self._buf.flush()

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
        if not self._writable():
            raise IOError("Unsupproted operation: truncate")
        self._buf.write(b"\0" * count)

    trim = zero

    # Private

    def _readable(self):
        return self._mode in ("r", "r+")

    def _writable(self):
        return self._mode in ("w", "r+")
