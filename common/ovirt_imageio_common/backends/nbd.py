# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import os

from .. import nbd

log = logging.getLogger("backends.nbd")

Error = nbd.Error

# Default buffer size for copying.
BUFFER_SIZE = 1024**2


def open(url, mode, sparse=True, buffer_size=BUFFER_SIZE):
    """
    Open a NBD backend.

    Arguments:
        url (url): parsed NBD url (e.g. "nbd:unix:/socket:exportname=name")
        mode (str): "r" for readonly, "w" for write only, "r+" for read write.
            Note that writeable backend does not guarantee that the underling
            nbd server is writable. The server must be configured to allow
            writing.
        sparse (bool): ignored, NBD backend does not support sparseness. This
            must be controlled by the nbd server. It seems that qemu-nbd and
            qemu always deallocate space when zeroing.
        buffer_size (int): size of buffer to allocate if needed.
    """
    client = nbd.open(url)
    try:
        return Backend(client, mode, buffer_size=buffer_size)
    except:  # noqa: E722
        client.close()
        raise


class Backend(object):
    """
    NBD backend.
    """

    def __init__(self, client, mode, buffer_size=BUFFER_SIZE):
        if mode not in ("r", "w", "r+"):
            raise ValueError("Unsupported mode %r" % mode)
        self._client = client
        self._mode = mode
        self._buffer = bytearray(buffer_size)
        self._position = 0
        self._dirty = False

    # Backend interface

    def readinto(self, buf):
        if not self.readable():
            raise IOError("Unsupported operation: readinto")

        # TODO: This is horrible - we copy the data allocated by the client to
        # the buffer allocated by the operation, instead of passing the buffer
        # to the client, and the client passing the buffer to the socket.
        # This can be fixed if we use bytearray instead of mmap when using nbd
        # client, and allocate the buffer by the backend instead of by the
        # operation.
        to_read = min(len(buf), self._client.export_size - self._position)
        data = self._client.read(self._position, to_read)
        length = len(data)
        buf[:length] = bytes(data)

        self._position += length
        return length

    def write_to(self, dst, length):
        """
        Copy up to length bytes at current position from NBD server to dst
        file-like object.
        """
        if not self.readable():
            raise IOError("Unsupported operation: write_to")

        max_step = min(self._client.maximum_block_size, len(self._buffer))
        end = self._position + length

        while self._position < end:
            step = min(end - self._position, max_step)
            buf = memoryview(self._buffer)[:step]
            self._client.readinto(self._position, buf)
            dst.write(buf)
            self._position += step

        return length

    def write(self, buf):
        if not self.writable():
            raise IOError("Unsupported operation: write")
        self._client.write(self._position, buf)
        length = len(buf)
        self._position += length
        self._dirty = True
        return length

    def zero(self, length):
        if not self.writable():
            raise IOError("Unsupported operation: zero")
        self._client.zero(self._position, length)
        self._position += length
        self._dirty = True
        return length

    def flush(self):
        self._client.flush()
        self._dirty = False

    def tell(self):
        return self._position

    def seek(self, pos, how=os.SEEK_SET):
        if how == os.SEEK_SET:
            self._position = pos
        elif how == os.SEEK_CUR:
            self._position += pos
        elif how == os.SEEK_END:
            self._position = self._client.export_size + pos
        return self._position

    def close(self):
        self._client.close()

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

    @property
    def block_size(self):
        # We may use minimum_block_size, but it is always 1 with qemu-nbd and
        # qemu, and I think it will break the ops code, expecting either 512 or
        # 4096. This will cause ops.Send/Receive to do first small unaligned
        # read/write to get the offset aligned. Both qemu and qemu-nbd seems to
        # handle unaligned reads and writes.
        return self._client.preferred_block_size

    # Debugging interface

    def readable(self):
        return self._mode in ("r", "r+")

    def writable(self):
        return self._mode in ("w", "r+")

    @property
    def dirty(self):
        """
        Returns True if backend was modified and needs flushing.
        """
        return self._dirty

    @property
    def sparse(self):
        # TODO:
        # - can we get this info from the backend?
        return True

    @property
    def name(self):
        return "nbd"

    def size(self):
        return self._client.export_size
