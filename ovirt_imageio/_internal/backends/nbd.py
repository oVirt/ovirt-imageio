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
from .. import extent
from .. import nbd
from .. import nbdutil

from . common import CLOSED

log = logging.getLogger("backends.nbd")

Error = nbd.Error


def open(url, mode="r", sparse=False, dirty=False, max_connections=8,
         **options):
    """
    Open a NBD backend.

    Arguments:
        url (url): parsed NBD url (e.g. "nbd:unix:/socket:exportname=name")
        mode (str): "r" for readonly, "w" for write only, "r+" for read write.
            Note that writeable backend does not guarantee that the underling
            nbd server is writable. The server must be configured to allow
            writing.
        sparse (bool): deallocate space when zeroing if possible.
        dirty (bool): if True, configure the client to report dirty extents.
            Can work only when connecting to qemu during incremental backup.
        max_connections (int): maximum number of connections per backend
            allowed on this server. Limit backends's max_readers and
            max_writers.
        **options: ignored, nbd backend does not have any options.
    """
    client = nbd.open(url, dirty=dirty)
    try:
        return Backend(
            client,
            mode=mode,
            sparse=sparse,
            max_connections=max_connections)
    except:  # noqa: E722
        client.close()
        raise


class Backend:
    """
    NBD backend.
    """

    def __init__(self, client, mode="r", sparse=False, max_connections=8):
        if mode not in ("r", "w", "r+"):
            raise ValueError("Unsupported mode %r" % mode)
        log.debug("Open address=%r export_name=%r sparse=%r "
                  "max_connections=%r",
                  client.address, client.export_name, sparse, max_connections)
        self._client = client
        self._mode = mode
        self._sparse = sparse
        self._position = 0
        self._dirty = False
        self._max_connections = max_connections

    def clone(self):
        """
        Return new backend connected to the same NBD export.
        """
        client = nbd.Client(
            self._client.address,
            export_name=self._client.export_name,
            dirty=self._client.dirty)
        try:
            return self.__class__(
                client,
                mode=self._mode,
                max_connections=self._max_connections)
        except:  # noqa: E722
            client.close()
            raise

    @property
    def max_readers(self):
        return self._max_connections

    @property
    def max_writers(self):
        return self._max_connections

    # Backend interface

    def readinto(self, buf):
        if not self.readable():
            raise IOError("Unsupported operation: readinto")

        length = min(len(buf), self._client.export_size - self._position)
        if length <= 0:
            # Client should not send NBD_CMD_READ with zero length, and
            # request after the end of file is invalid.
            return 0

        with memoryview(buf)[:length] as view:
            self._client.readinto(self._position, view)

        self._position += length
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
        self._client.zero(self._position, length, punch_hole=self._sparse)
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
        if self._client is not CLOSED:
            log.debug("Close address=%r", self._client.address)
            self._client.close()
            self._client = CLOSED

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

    def extents(self, context="zero"):
        if context not in ("zero", "dirty"):
            raise errors.UnsupportedOperation(
                "Backend nbd does not support {} extents".format(context))

        # If server does not support base:allocation, we can safely report one
        # data extent like other backends.
        if context == "zero" and not self._client.has_base_allocation:
            yield extent.ZeroExtent(0, self._client.export_size, False, False)
            return

        # If dirty extents are not available, client may be able to use zero
        # extents for eficient download, so we should not fake the response.
        if context == "dirty" and self._client.dirty_bitmap is None:
            raise errors.UnsupportedOperation(
                "NBD export {!r} does not support dirty extents"
                .format(self._client.export_name))

        dirty = context == "dirty"
        start = 0
        for ext in nbdutil.extents(self._client, dirty=dirty):
            if dirty:
                yield extent.DirtyExtent(
                    start, ext.length, ext.dirty, ext.zero)
            else:
                yield extent.ZeroExtent(
                    start, ext.length, ext.zero, ext.hole)
            start += ext.length

    @property
    def block_size(self):
        # qemu always reports minium_block_size=1, so caller never needs to
        # align requests and read more data than needed.
        return self._client.minimum_block_size

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
        return self._sparse

    @property
    def name(self):
        return "nbd"

    def size(self):
        return self._client.export_size
