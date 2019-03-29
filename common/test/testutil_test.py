# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import socket
import time

from contextlib import closing

from ovirt_imageio_common import nbd

from . import testutil


def test_unbuffered_stream_more():
    chunks = [b"1" * 256,
              b"2" * 256,
              b"3" * 42,
              b"4" * 256]
    s = testutil.UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(512)
    assert b == chunks[0]
    # Chunk 2
    b = s.read(512)
    assert b == chunks[1]
    # Chunk 3
    b = s.read(512)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(512)
    assert b == chunks[3]
    # Empty
    b = s.read(512)
    assert b == b''
    b = s.read(512)
    assert b == b''


def test_unbuffered_stream_less():
    chunks = [b"1" * 256,
              b"2" * 256,
              b"3" * 42,
              b"4" * 256]
    s = testutil.UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(128)
    assert b == chunks[0][:128]
    b = s.read(128)
    assert b == chunks[0][128:]
    # Chunk 2
    b = s.read(128)
    assert b == chunks[1][:128]
    b = s.read(128)
    assert b == chunks[1][128:]
    # Chunk 3
    b = s.read(128)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(128)
    assert b == chunks[3][:128]
    b = s.read(128)
    assert b == chunks[3][128:]
    # Empty
    b = s.read(128)
    assert b == b''
    b = s.read(128)
    assert b == b''


def test_wait_for_unix_socket(tmpdir):
    addr = nbd.UnixAddress(tmpdir.join("path"))

    # Socket was not created yet.
    start = time.time()
    assert not testutil.wait_for_socket(addr, 0.1)
    waited = time.time() - start
    assert 0.1 <= waited < 0.2

    sock = socket.socket(socket.AF_UNIX)
    with closing(sock):
        sock.bind(addr)

        # Socket bound but not listening yet.
        start = time.time()
        assert not testutil.wait_for_socket(addr, 0.1)
        waited = time.time() - start
        assert 0.1 <= waited < 0.2

        sock.listen(1)

        # Socket listening - should return immediately.
        assert testutil.wait_for_socket(addr, 0.0)

    # Socket was closed - should return immediately.
    assert not testutil.wait_for_socket(addr, 0.0)


def test_wait_for_tcp_socket():
    sock = socket.socket()
    with closing(sock):
        sock.bind(("localhost", 0))
        addr = nbd.TCPAddress(*sock.getsockname())

        # Socket bound but not listening yet.
        start = time.time()
        assert not testutil.wait_for_socket(addr, 0.1)
        waited = time.time() - start
        assert 0.1 <= waited < 0.2

        sock.listen(1)

        # Socket listening - should return immediately.
        assert testutil.wait_for_socket(addr, 0.0)

    # Socket was closed - should return immediately.
    assert not testutil.wait_for_socket(addr, 0.0)
