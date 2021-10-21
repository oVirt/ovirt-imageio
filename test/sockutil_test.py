# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import time
import socket

from contextlib import closing

from ovirt_imageio._internal import sockutil


def test_wait_for_unix_socket(tmpdir):
    addr = sockutil.UnixAddress(tmpdir.join("path"))

    # Socket was not created yet.
    start = time.monotonic()
    assert not sockutil.wait_for_socket(addr, 0.1)
    waited = time.monotonic() - start
    assert 0.1 <= waited < 0.2

    sock = socket.socket(socket.AF_UNIX)
    with closing(sock):
        sock.bind(addr)

        # Socket bound but not listening yet.
        start = time.monotonic()
        assert not sockutil.wait_for_socket(addr, 0.1)
        waited = time.monotonic() - start
        assert 0.1 <= waited < 0.2

        sock.listen(1)

        # Socket listening - should return immediately.
        assert sockutil.wait_for_socket(addr, 0.0)

    # Socket was closed - should return immediately.
    assert not sockutil.wait_for_socket(addr, 0.0)


def test_wait_for_tcp_socket():
    sock = socket.socket()
    with closing(sock):
        sock.bind(("localhost", 0))
        addr = sockutil.TCPAddress(*sock.getsockname())

        # Socket bound but not listening yet.
        start = time.monotonic()
        assert not sockutil.wait_for_socket(addr, 0.1)
        waited = time.monotonic() - start
        assert 0.1 <= waited < 0.2

        sock.listen(1)

        # Socket listening - should return immediately.
        assert sockutil.wait_for_socket(addr, 0.0)

    # Socket was closed - should return immediately.
    assert not sockutil.wait_for_socket(addr, 0.0)
