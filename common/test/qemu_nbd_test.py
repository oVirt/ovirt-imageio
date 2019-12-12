# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io

from six.moves import urllib_parse

import pytest

from ovirt_imageio_common import nbd

from . import qemu_img
from . import qemu_nbd
from . import testutil


@pytest.mark.parametrize("addr,export,url", [
    (nbd.UnixAddress("/sock"), None, "nbd:unix:/sock"),
    (nbd.UnixAddress("/sock"), "", "nbd:unix:/sock"),
    (nbd.UnixAddress("/sock"), "sda", "nbd:unix:/sock:exportname=sda"),
    (nbd.TCPAddress("host", 10900), None, "nbd:host:10900"),
    (nbd.TCPAddress("host", 10900), "", "nbd:host:10900"),
    (nbd.TCPAddress("host", 10900), "sdb", "nbd:host:10900:exportname=sdb"),
])
def test_server_url(addr, export, url):
    srv = qemu_nbd.Server("image", "raw", addr, export_name=export)
    assert srv.url == urllib_parse.urlparse(url)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_open(tmpdir, fmt):
    disk = str(tmpdir.join("disk." + fmt))
    qemu_img.create(disk, fmt, size=1024**2)

    offset = 64 * 1024
    data = b"it works"

    with qemu_nbd.open(disk, fmt) as d:
        d.write(offset, data)
        d.flush()

    with qemu_nbd.open(disk, fmt, read_only=True) as d:
        assert d.read(offset, len(data)) == data


def test_run_unix(tmpdir):
    image = str(tmpdir.join("image"))
    sock = str(tmpdir.join("sock"))

    with io.open(image, "wb") as f:
        f.truncate(1024**2)

    addr = nbd.UnixAddress(sock)

    with qemu_nbd.run(image, "raw", addr):
        # The helper already waited for the NBD socket, not wait needed.
        assert testutil.wait_for_socket(addr, 0.0)

    # The socket must be closed, no wait needed.
    assert not testutil.wait_for_socket(addr, 0.0)


def test_run_tcp(tmpfile):
    with io.open(tmpfile, "r+b") as f:
        f.truncate(1024**2)

    addr = nbd.TCPAddress("localhost", testutil.random_tcp_port())

    with qemu_nbd.run(tmpfile, "raw", addr):
        # The helper already waited for the NBD socket, not wait needed.
        assert testutil.wait_for_socket(addr, 0.0)

    # The socket must be closed, no wait needed.
    assert not testutil.wait_for_socket(addr, 0.0)
