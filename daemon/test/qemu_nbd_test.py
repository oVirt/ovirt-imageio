# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import struct

from six.moves import urllib_parse

import pytest

from ovirt_imageio_common import nbd
from ovirt_imageio_common import nbdutil
from ovirt_imageio_common import qemu_img
from ovirt_imageio_common import qemu_nbd

from . import testutil
from . marks import requires_python3

pytestmark = requires_python3


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
        assert nbdutil.wait_for_socket(addr, 0.0)

    # The socket must be closed, no wait needed.
    assert not nbdutil.wait_for_socket(addr, 0.0)


def test_run_tcp(tmpfile):
    with io.open(tmpfile, "r+b") as f:
        f.truncate(1024**2)

    addr = nbd.TCPAddress("localhost", testutil.random_tcp_port())

    with qemu_nbd.run(tmpfile, "raw", addr):
        # The helper already waited for the NBD socket, not wait needed.
        assert nbdutil.wait_for_socket(addr, 0.0)

    # The socket must be closed, no wait needed.
    assert not nbdutil.wait_for_socket(addr, 0.0)


@pytest.mark.parametrize("cache,aio,discard", [
    pytest.param("none", "native", "unmap", id="qemu_nbd defaults"),
    pytest.param(None, None, None, id="qemu-nbd defaults"),
])
@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_options(tmpdir, fmt, cache, aio, discard):
    size = 4 * 1024**2
    chunk_size = 128 * 1024

    src = str(tmpdir.join("src." + fmt))
    qemu_img.create(src, fmt, size=size)

    with qemu_nbd.open(src, fmt) as c:
        for offset in range(0, size, chunk_size):
            c.write(offset, struct.pack(">Q", offset))
        c.flush()

    dst = str(tmpdir.join("dst." + fmt))
    qemu_img.create(dst, fmt, size=size)

    src_addr = nbd.UnixAddress(str(tmpdir.join("src.sock")))
    dst_addr = nbd.UnixAddress(str(tmpdir.join("dst.sock")))

    with qemu_nbd.run(
                src, fmt, src_addr,
                read_only=True,
                cache=cache,
                aio=aio,
                discard=discard), \
            qemu_nbd.run(
                dst, fmt, dst_addr,
                cache=cache,
                aio=aio,
                discard=discard), \
            nbd.Client(src_addr) as src_client, \
            nbd.Client(dst_addr) as dst_client:

        nbdutil.copy(src_client, dst_client)

    qemu_img.compare(src, dst)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_shared(tmpdir, fmt):
    size = 1024**2
    chunk_size = size // 2

    src = str(tmpdir.join("src." + fmt))
    qemu_img.create(src, fmt, size=size)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"a" * chunk_size)
        c.write(0, b"b" * chunk_size)
        c.flush()

    dst = str(tmpdir.join("dst." + fmt))
    qemu_img.create(dst, fmt, size=size)

    src_addr = nbd.UnixAddress(str(tmpdir.join("src.sock")))
    dst_addr = nbd.UnixAddress(str(tmpdir.join("dst.sock")))

    # Start 2 qemu-nbd servers, each with 2 clients that can read and write in
    # parallel for higher throughput.

    with qemu_nbd.run(src, fmt, src_addr, read_only=True, shared=2), \
            qemu_nbd.run(dst, fmt, dst_addr, shared=2), \
            nbd.Client(src_addr) as src_client1, \
            nbd.Client(src_addr) as src_client2, \
            nbd.Client(dst_addr) as dst_client1, \
            nbd.Client(dst_addr) as dst_client2:

        # Copy first half of the image with src_client1 and dst_client2 and
        # second half with src_client2 and dst_client2. In a real application
        # we would have more clients, running in multiple threads.

        chunk1 = src_client1.read(0, chunk_size)
        dst_client1.write(0, chunk1)

        chunk2 = src_client2.read(chunk_size, chunk_size)
        dst_client2.write(chunk_size, chunk2)

        dst_client1.flush()
        dst_client2.flush()

    qemu_img.compare(src, dst)
