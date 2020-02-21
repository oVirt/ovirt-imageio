# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import json
import logging

from six.moves.urllib_parse import urlparse
import pytest

from ovirt_imageio import http
from ovirt_imageio import ssl
from ovirt_imageio import uhttp
from ovirt_imageio import util
from ovirt_imageio import errors

from ovirt_imageio.backends import image
from ovirt_imageio.backends.http import Backend

from . marks import requires_python3

pytestmark = requires_python3

log = logging.getLogger("test")


@pytest.fixture(scope="module")
def http_server(tmp_pki):
    server = http.Server(("localhost", 0), http.Connection)
    log.info("Server listening on port %d", server.server_port)

    ctx = ssl.server_context(tmp_pki.cafile, tmp_pki.certfile, tmp_pki.keyfile)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)

    server.url = urlparse(
        "https://localhost:{}/".format(server.server_port))
    server.cafile = tmp_pki.cafile
    server.app = http.Router([])

    t = util.start_thread(
        server.serve_forever,
        kwargs={"poll_interval": 0.1})

    yield server

    server.shutdown()
    t.join()


@pytest.fixture(scope="module")
def uhttp_server(tmp_pki):
    server = uhttp.Server("", uhttp.Connection)
    log.info("Server listening on %r", server.server_address)

    server.app = http.Router([])

    t = util.start_thread(
        server.serve_forever,
        kwargs={"poll_interval": 0.1})

    yield server

    server.shutdown()
    t.join()


# Server personalies.

class Handler(object):

    def __init__(self, http_server, uhttp_server=None):
        self.features = []
        self.unix_socket = None
        self.dirty = False
        self.requests = 0

        router = http.Router([("/(.*)", self)])
        http_server.app = router
        if uhttp_server:
            uhttp_server.app = router
            self.unix_socket = uhttp_server.server_address

        # Number clusters to make sure get return real data from the server,
        # and put modify server data.
        self.image = bytearray(1024**2)
        for offset in range(0, len(self.image), 64 * 1024):
            self.image[offset] = offset % 256

    def get(self, req, resp, path=None):
        """
        Implement GET with optional Range header.
        """
        self.requests += 1
        if req.range:
            offset = req.range.first
            size = req.range.last + 1 - offset
            resp.status_code = http.PARTIAL_CONTENT
            resp.headers["content-range"] = "bytes {}-{}/{}".format(
                offset, offset + size - 1, len(self.image))
        else:
            offset = 0
            size = len(self.image)
            resp.status_code = http.OK

        resp.headers["content-length"] = size
        self._read(resp, offset, size)

    def put(self, req, resp, path=None):
        """
        Implement PUT with optional Content-Range header.
        """
        self.requests += 1
        offset = req.content_range.first if req.content_range else 0
        size = req.content_length
        self._write(req, offset, size, flush=True)

    def _read(self, resp, offset, size):
        log.debug("READ offset=%s size=%s", offset, size)
        with memoryview(self.image)[offset:offset + size] as view:
            resp.write(view)

    def _write(self, req, offset, size, flush):
        log.debug("WRITE offset=%s size=%s flush=%s", offset, size, flush)
        self.image[offset:offset + size] = req.read()
        self.dirty = not flush


class OldDaemon(Handler):
    """
    Older daemon supported only GET and PUT.
    """


class OldProxy(Handler):
    """
    Old proxy supported GET and PUT, and incompatible version of OPTIONS
    returning non content.
    """

    def options(self, req, resp, ticket_id):
        resp.status_code = http.NO_CONTENT


class Daemon(Handler):
    """
    Modern daemon added OPTIONS exposing capabilities, PATCH/zero, PATCH/flush
    and recently /extents resource.
    """

    def __init__(self, http_server, uhttp_server=None, extents=True):
        super().__init__(http_server, uhttp_server)

        # zero and flush support was introduce with OPTIONS, so we always
        # support both.
        self.features = ["zero", "flush"]
        if extents:
            self.features.append("extents")

        # Extents support was added later. It works only with NBD backend, and
        # emulated otherwise by reporting single non-zero extent.
        # "zero" extents are always available via emulation. "dirty" extents
        # are available only during incremental backup.
        self.extents = {
            "zero": [{"start": 0, "length": len(self.image), "zero": True}]
        }

    def options(self, req, resp, path=None):
        """
        Implement OPTIONS.
        """
        self.requests += 1
        log.debug("OPTIONS path=%s", path)
        options = {"features": self.features}
        if self.unix_socket:
            options["unix_socket"] = self.unix_socket
        resp.send_json(options)

    def get(self, req, resp, path=None):
        """
        Override to dispatch "GET /extents" resource.
        """
        if path == "/extents":
            self.requests += 1
            context = req.query.get("context", "zero")
            self._extents(resp, context)
        else:
            super().get(req, resp, path)

    def put(self, req, resp, path=None):
        """
        Implement PUT /?flush=y|n
        """
        self.requests += 1
        offset = req.content_range.first if req.content_range else 0
        size = req.content_length
        flush = req.query.get("flush") == "y"
        self._write(req, offset, size, flush=flush)

    def patch(self, req, resp, path=None):
        """
        Implement PATCH/zero and PATCH/flush.
        """
        self.requests += 1
        msg = json.loads(req.read())
        if msg["op"] == "zero":
            self._zero(msg)
        elif msg["op"] == "flush":
            self._flush()
        else:
            raise http.Error(http.BAD_REQUEST, "Invalid PATCH request")

    def _extents(self, resp, context):
        # Older daemon considered "/extents" as part of the ticket id, and will
        # fail to authorize the request.
        if "extents" not in self.features:
            raise http.Error(http.FORBIDDEN, "No extents for you!")
        if context not in self.extents:
            raise http.Error(http.NOT_FOUND, "No dirty extents for you!")
        log.debug("EXTENTS context=%s", context)
        resp.send_json(self.extents[context])

    def _zero(self, msg):
        offset = msg["offset"]
        size = msg["size"]
        flush = msg["flush"]

        log.debug("ZERO offset=%s size=%s flush=%s", offset, size, flush)

        self.image[offset:offset + size] = b"\0" * size
        self.dirty = not flush

    def _flush(self):
        log.debug("FLUSH")
        self.dirty = False


# Old daemon tests

def test_old_daemon_open(http_server):
    handler = OldDaemon(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        assert b.server_address == http_server.server_address
        assert b.tell() == 0
        assert b.size() == len(handler.image)

        # Client emulates extents (all non-zero).
        assert list(b.extents()) == [image.ZeroExtent(0, b.size(), False)]


def test_old_daemon_readinto(http_server):
    handler = OldDaemon(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_readinto(handler, b)


def test_old_daemon_write(http_server):
    handler = OldDaemon(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write(handler, b)
        assert not handler.dirty


def test_old_daemon_zero(http_server):
    handler = OldDaemon(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_zero(handler, b)
        assert not handler.dirty


def test_old_daemon_read_from(http_server):
    handler = OldDaemon(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_read_from(handler, b)
        assert not handler.dirty


def test_old_daemon_write_to(http_server):
    handler = OldDaemon(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write_to(handler, b)


# Old proxy tests

def test_old_proxy_open(http_server):
    handler = OldProxy(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        assert b.server_address == http_server.server_address
        assert b.tell() == 0
        assert b.size() == len(handler.image)

        # Client emulates extents (all non-zero).
        assert list(b.extents()) == [image.ZeroExtent(0, b.size(), False)]


def test_old_proxy_readinto(http_server):
    handler = OldProxy(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_readinto(handler, b)


def test_old_proxy_write(http_server):
    handler = OldProxy(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write(handler, b)
        assert not handler.dirty


def test_old_proxy_zero(http_server):
    handler = OldProxy(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_zero(handler, b)
        assert not handler.dirty


def test_old_proxy_read_from(http_server):
    handler = OldProxy(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_read_from(handler, b)
        assert not handler.dirty


def test_old_proxy_write_to(http_server):
    handler = OldProxy(http_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write_to(handler, b)


# Daemon without unix socket tests

def test_daemon_no_unix_socket_open(http_server):
    handler = Daemon(http_server, extents=False)
    with Backend(http_server.url, http_server.cafile) as b:
        assert b.server_address == http_server.server_address
        assert b.tell() == 0
        assert b.size() == len(handler.image)

        # Client emulates extents (all non-zero).
        assert list(b.extents()) == [image.ZeroExtent(0, b.size(), False)]


def test_daemon_no_unix_socket_readinto(http_server):
    handler = Daemon(http_server, extents=False)
    with Backend(http_server.url, http_server.cafile) as b:
        check_readinto(handler, b)


def test_daemon_no_unix_socket_write(http_server):
    handler = Daemon(http_server, extents=False)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write(handler, b)
        assert handler.dirty
        b.flush()
        assert not handler.dirty


def test_daemon_no_unix_socket_zero(http_server):
    handler = Daemon(http_server, extents=False)
    with Backend(http_server.url, http_server.cafile) as b:
        check_zero(handler, b)
        assert handler.dirty
        b.flush()
        assert not handler.dirty


def test_daemon_no_unix_socket_read_from(http_server):
    handler = Daemon(http_server, extents=False)
    with Backend(http_server.url, http_server.cafile) as b:
        check_read_from(handler, b)
        assert handler.dirty
        b.flush()
        assert not handler.dirty


def test_daemon_no_unix_socket_write_to(http_server):
    handler = Daemon(http_server, extents=False)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write_to(handler, b)


# Daemon with bad unix socket tests

def test_daemon_bad_unix_socket_open(http_server):
    handler = Daemon(http_server, extents=False)
    handler.unix_socket = "\0bad/socket"
    with Backend(http_server.url, http_server.cafile) as b:
        assert b.server_address == http_server.server_address
        assert b.tell() == 0
        assert b.size() == len(handler.image)

        # Client emulates extents (all non-zero).
        assert list(b.extents()) == [image.ZeroExtent(0, b.size(), False)]


# Daemon with no extents support tests

def test_daemon_no_extents_open(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server, extents=False)
    with Backend(http_server.url, http_server.cafile) as b:
        assert b.server_address == uhttp_server.server_address
        assert b.tell() == 0
        assert b.size() == len(handler.image)

        # Client emulates extents (all non-zero).
        assert list(b.extents()) == [image.ZeroExtent(0, b.size(), False)]


# Daemon tests

def test_daemon_open(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)
    with Backend(http_server.url, http_server.cafile) as b:
        assert b.server_address == uhttp_server.server_address
        assert b.tell() == 0
        assert b.size() == len(handler.image)

        # Client reports server extents (all zero).
        assert list(b.extents()) == [image.ZeroExtent(0, b.size(), True)]


def test_daemon_open_insecure(http_server, uhttp_server):
    Daemon(http_server, uhttp_server)
    with Backend(http_server.url, None, secure=False) as b:
        assert b.server_address == uhttp_server.server_address


def test_daemon_extents_zero(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)

    chunk_size = len(handler.image) // 2
    handler.extents["zero"] = [
        {"start": 0, "length": chunk_size, "zero": False},
        {"start": chunk_size, "length": chunk_size, "zero": True},
    ]

    with Backend(http_server.url, http_server.cafile) as b:
        # Zero extents are available.
        assert list(b.extents()) == [
            image.ZeroExtent(0, chunk_size, False),
            image.ZeroExtent(chunk_size, chunk_size, True),
        ]

        # Dirty extents are not available.
        with pytest.raises(errors.UnsupportedOperation):
            list(b.extents("dirty"))


def test_daemon_extents_dirty(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)

    chunk_size = len(handler.image) // 2
    handler.extents["dirty"] = [
        {"start": 0, "length": chunk_size, "dirty": True},
        {"start": chunk_size, "length": chunk_size, "dirty": False},
    ]

    with Backend(http_server.url, http_server.cafile) as b:
        # Both "zero" and "dirty" extents are available.
        assert list(b.extents("zero")) == [
            image.ZeroExtent(0, b.size(), True),
        ]
        assert list(b.extents("dirty")) == [
            image.DirtyExtent(0, chunk_size, True),
            image.DirtyExtent(chunk_size, chunk_size, False),
        ]


def test_daemon_readinto(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_readinto(handler, b)


def test_daemon_write(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write(handler, b)
        assert handler.dirty
        b.flush()
        assert not handler.dirty


def test_daemon_zero(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_zero(handler, b)
        assert handler.dirty
        b.flush()
        assert not handler.dirty


def test_daemon_read_from(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_read_from(handler, b)
        assert handler.dirty
        b.flush()
        assert not handler.dirty


def test_daemon_write_to(http_server, uhttp_server):
    handler = Daemon(http_server, uhttp_server)
    with Backend(http_server.url, http_server.cafile) as b:
        check_write_to(handler, b)


# Common flows - must works for all variants.

def check_readinto(handler, backend):
    """
    Check single readinto opertion.
    """
    offset = 8192
    length = 4096
    buf = bytearray(length)

    backend.seek(offset)
    backend.readinto(buf)

    assert backend.tell() == offset + length
    assert buf == handler.image[offset:offset + length]


def check_write(handler, backend):
    """
    Check single write opertion.
    """
    offset = 8192
    length = 4096
    buf = bytearray(b"x" * length)

    backend.seek(offset)
    backend.write(buf)

    assert backend.tell() == offset + length
    assert buf == handler.image[offset:offset + length]


def check_zero(handler, backend):
    """
    Check single zero opertion.
    """
    offset = 8192
    length = 512 * 1024

    backend.seek(offset)
    backend.zero(length)

    assert backend.tell() == offset + length
    assert handler.image[offset:offset + length] == b"\0" * length


def check_read_from(handler, backend):
    """
    Check that we can stream data using small buffer with a single request.
    The length is intentionally not a multiple of the internal buffer size to
    check handling of partial buffers.
    """
    offset = 8192
    length = 600000
    reader = io.BytesIO(b"x" * length)
    buf = bytearray(128 * 1024)
    handler.requests = 0

    backend.seek(offset)
    backend.read_from(reader, length, buf)

    assert handler.requests == 1
    assert backend.tell() == offset + length
    assert handler.image[offset:offset + length] == reader.getvalue()


def check_write_to(handler, backend):
    """
    Check that we can stream data using small buffer with a single request.
    The length is intentionally not a multiple of the internal buffer size to
    check handling of partial buffers.
    """
    offset = 8192
    length = 600000
    writer = io.BytesIO(b"x" * length)
    buf = bytearray(128 * 1024)
    handler.requests = 0

    backend.seek(offset)
    backend.write_to(writer, length, buf)

    assert handler.requests == 1
    assert backend.tell() == offset + length
    assert writer.getvalue() == handler.image[offset:offset + length]
