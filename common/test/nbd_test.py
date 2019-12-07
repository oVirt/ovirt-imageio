# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import io
import logging
import os

from six.moves.urllib_parse import urlparse

import pytest
import userstorage

from ovirt_imageio_common import nbd
from ovirt_imageio_common import nbdutil
from ovirt_imageio_common.compat import subprocess

from . import qemu_nbd
from . import backup
from . import testutil
from . import storage

BACKENDS = userstorage.load_config("../storage.py").BACKENDS

log = logging.getLogger("test")


@pytest.fixture(
    params=[
        BACKENDS["file-512-ext4"],
        BACKENDS["file-512-xfs"],
        BACKENDS["file-4k-ext4"],
        BACKENDS["file-4k-xfs"],
    ],
    ids=str
)
def user_file(request):
    with storage.Backend(request.param) as backend:
        yield backend


# Addresses

@pytest.mark.parametrize("addr,export,url", [
    # Note: We get Unicode when parsing ticket JSON.
    (nbd.UnixAddress(u"/sock"), None, u"nbd:unix:/sock"),
    (nbd.UnixAddress(u"/sock"), u"", u"nbd:unix:/sock"),
    (nbd.UnixAddress(u"/sock"), u"sda", u"nbd:unix:/sock:exportname=sda"),
    (nbd.TCPAddress(u"host", 10900), None, u"nbd:host:10900"),
    (nbd.TCPAddress(u"host", 10900), u"", u"nbd:host:10900"),
    (nbd.TCPAddress(u"host", 10900), u"sdb", u"nbd:host:10900:exportname=sdb"),
])
def test_url(addr, export, url):
    assert addr.url(export) == url


@pytest.mark.parametrize("host,port", [
    (u"localhost", u"42"),
    (42, 42),
])
def test_tcp_address_invalid(host, port):
    with pytest.raises(ValueError):
        nbd.TCPAddress(host, port)


# Communicate with qemu-nbd


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
@pytest.mark.parametrize("export", [
    "",
    "ascii",
    pytest.param(u"\u05d0", id="unicode"),
])
def test_handshake(tmpdir, export, fmt):
    image = str(tmpdir.join("image"))
    subprocess.check_call(["qemu-img", "create", "-f", fmt, image, "1g"])
    sock = nbd.UnixAddress(tmpdir.join("sock"))

    with qemu_nbd.run(image, fmt, sock, export_name=export):
        if export:
            c = nbd.Client(sock, export)
        else:
            c = nbd.Client(sock)
        with c:
            # TODO: test transmission_flags?
            assert c.export_size == 1024**3
            assert c.minimum_block_size == 1
            assert c.preferred_block_size == 4096
            assert c.maximum_block_size == 32 * 1024**2
            assert c.base_allocation


def test_raw_read(tmpdir):
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    offset = 1024**2
    data = b"can read from raw"

    with open(image, "wb") as f:
        f.truncate(1024**3)
        f.seek(offset)
        f.write(data)

    with qemu_nbd.run(image, "raw", sock):
        with nbd.Client(sock) as c:
            assert c.read(offset, len(data)) == data


def test_raw_readinto(nbd_server):
    offset = 1024**2
    data = b"can read from raw"

    with io.open(nbd_server.image, "wb") as f:
        f.truncate(2 * 1024**2)
        f.seek(1024**2)
        f.write(data)

    nbd_server.start()

    with nbd.open(nbd_server.url) as c:
        buf = bytearray(len(data))
        c.readinto(offset, buf)
        assert buf == data


def test_raw_write(tmpdir):
    image = str(tmpdir.join("image"))
    with open(image, "wb") as f:
        f.truncate(1024**3)
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    offset = 1024**2
    data = b"can write to raw"

    with qemu_nbd.run(image, "raw", sock):
        with nbd.Client(sock) as c:
            c.write(offset, data)
            c.flush()

    with open(image, "rb") as f:
        f.seek(offset)
        assert f.read(len(data)) == data


def test_qcow2_write_read(tmpdir):
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    offset = 1024**2
    data = b"can read and write qcow2"
    subprocess.check_call(["qemu-img", "create", "-f", "qcow2", image, "1g"])

    with qemu_nbd.run(image, "qcow2", sock):
        with nbd.Client(sock) as c:
            c.write(offset, data)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, len(data)) == data


@pytest.mark.parametrize("format", ["raw", "qcow2"])
def test_zero(tmpdir, format):
    size = 2 * 1024**2
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    subprocess.check_call(
        ["qemu-img", "create", "-f", format, image, str(size)])

    with qemu_nbd.run(image, format, sock):
        # Fill image with data
        with nbd.Client(sock) as c:
            c.write(0, b"x" * size)
            c.flush()

        # Zero a range
        with nbd.Client(sock) as c:
            c.zero(offset, 4096)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, 4096) == b"\0" * 4096


@pytest.mark.parametrize("format", ["raw", "qcow2"])
def test_zero_max_block_size(tmpdir, format):
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    subprocess.check_call(
        ["qemu-img", "create", "-f", format, image, "1g"])

    with qemu_nbd.run(image, format, sock):
        # Fill range with data
        with nbd.Client(sock) as c:
            size = c.maximum_block_size
            c.write(offset, b"x" * size)
            c.flush()

        # Zero range using maximum block size
        with nbd.Client(sock) as c:
            c.zero(offset, size)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, size) == b"\0" * size


@pytest.mark.parametrize("format", ["raw", "qcow2"])
def test_zero_min_block_size(tmpdir, format):
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    subprocess.check_call(
        ["qemu-img", "create", "-f", format, image, "1g"])

    with qemu_nbd.run(image, format, sock):
        # Fill range with data
        with nbd.Client(sock) as c:
            size = c.minimum_block_size
            c.write(offset, b"x" * size)
            c.flush()

        # Zero range using minimum block size
        with nbd.Client(sock) as c:
            c.zero(offset, size)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, size) == b"\0" * size


@pytest.mark.parametrize("url,export", [
    # Note: We get Unicode URL when parsing ticket JSON.
    (u"nbd:unix:/path", u""),
    (u"nbd:unix:/path:exportname=", u""),
    (u"nbd:unix:/path:exportname=sda", u"sda"),
    (u"nbd:unix:/path:exportname=/sda", u"/sda"),
])
def test_open_unix(tmpdir, url, export):
    image = str(tmpdir.join("image"))
    with open(image, "wb") as f:
        f.truncate(1024**3)

    sock = nbd.UnixAddress(tmpdir.join("sock"))
    url = url.replace("/path", sock)

    log.debug("Trying url=%r export=%r", url, export)
    with qemu_nbd.run(image, "raw", sock, export_name=export):
        with nbd.open(urlparse(url)) as c:
            assert c.export_size == 1024**3


@pytest.mark.parametrize("url_template,export", [
    # Note: We get Unicode URL when parsing ticket JSON.
    (u"nbd:localhost:{port}", u""),
    (u"nbd:localhost:{port}:exportname=", u""),
    (u"nbd:localhost:{port}:exportname=sda", u"sda"),
    (u"nbd:localhost:{port}:exportname=/sda", u"/sda"),
    (u"nbd://localhost:{port}", u""),
    (u"nbd://localhost:{port}/", u""),
    (u"nbd://localhost:{port}/sda", u"sda"),
])
def test_open_tcp(tmpdir, url_template, export):
    image = str(tmpdir.join("image"))
    with open(image, "wb") as f:
        f.truncate(1024**3)

    sock = nbd.TCPAddress(u"localhost", testutil.random_tcp_port())
    url = url_template.format(port=sock.port)

    log.debug("Trying url=%r export=%r", url, export)
    with qemu_nbd.run(image, "raw", sock, export_name=export):
        with nbd.open(urlparse(url)) as c:
            assert c.export_size == 1024**3


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_base_allocation_empty(nbd_server, user_file, fmt):
    size = nbd.MAX_LENGTH
    create_image(user_file.path, fmt, size)

    nbd_server.image = user_file.path
    nbd_server.fmt = fmt
    nbd_server.start()

    with nbd.open(nbd_server.url) as c:
        # Entire image.
        extents = c.extents(0, size)["base:allocation"]
        assert extents == [nbd.Extent(length=size, zero=True)]

        # First block.
        extents = c.extents(0, 4096)["base:allocation"]
        assert extents == [nbd.Extent(length=4096, zero=True)]

        # Last block.
        extents = c.extents(size - 4096, 4096)["base:allocation"]
        assert extents == [nbd.Extent(length=4096, zero=True)]

        # Some block.
        extents = c.extents(4096, 4096)["base:allocation"]
        assert extents == [nbd.Extent(length=4096, zero=True)]

        # Unaligned start.
        extents = c.extents(4096 - 1, 4096 + 1)["base:allocation"]
        assert extents == [nbd.Extent(length=4096 + 1, zero=True)]

        # Unaligned end.
        extents = c.extents(4096, 4096 + 1)["base:allocation"]
        assert extents == [nbd.Extent(length=4096 + 1, zero=True)]

        # Unaligned start and end.
        extents = c.extents(4096 - 1, 4096 + 2)["base:allocation"]
        assert extents == [nbd.Extent(length=4096 + 2, zero=True)]


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_base_allocation_full(nbd_server, user_file, fmt):
    size = 1024**2
    create_image(user_file.path, fmt, size)

    nbd_server.image = user_file.path
    nbd_server.fmt = fmt
    nbd_server.start()

    with nbd.open(nbd_server.url) as c:
        c.write(0, b"x" * size)

        # Entire image.
        extents = c.extents(0, size)["base:allocation"]
        assert extents == [nbd.Extent(length=size, zero=False)]

        # First block.
        extents = c.extents(0, 4096)["base:allocation"]
        assert extents == [nbd.Extent(length=4096, zero=False)]

        # Last block.
        extents = c.extents(size - 4096, 4096)["base:allocation"]
        assert extents == [nbd.Extent(length=4096, zero=False)]

        # Some block.
        extents = c.extents(4096, 4096)["base:allocation"]
        assert extents == [nbd.Extent(length=4096, zero=False)]

        # Unaligned start.
        extents = c.extents(4096 - 1, 4096 + 1)["base:allocation"]
        assert extents == [nbd.Extent(length=4096 + 1, zero=False)]

        # Unaligned end.
        extents = c.extents(4096, 4096 + 1)["base:allocation"]
        assert extents == [nbd.Extent(length=4096 + 1, zero=False)]

        # Unaligned start and end.
        extents = c.extents(4096 - 1, 4096 + 2)["base:allocation"]
        assert extents == [nbd.Extent(length=4096 + 2, zero=False)]


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_base_allocation_some_data(nbd_server, user_file, fmt):
    size = 1024**3
    create_image(user_file.path, fmt, size)

    nbd_server.image = user_file.path
    nbd_server.fmt = fmt
    nbd_server.start()

    # Use qcow2 cluster size to avoid inconsistent results on CentOS and
    # Fedora.
    data_length = 64 * 1024
    zero_length = size // 2 - data_length

    with nbd.open(nbd_server.url) as c:
        # Create 4 extents: data, zero, data, zero.
        c.write(0, b"x" * data_length)
        c.write(size // 2, b"x" * data_length)

        extents = list(nbdutil.extents(c))

    assert extents == [
        nbd.Extent(length=data_length, zero=False),
        nbd.Extent(length=zero_length, zero=True),
        nbd.Extent(length=data_length, zero=False),
        nbd.Extent(length=zero_length, zero=True),
    ]


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_base_allocation_some_data_unaligned(nbd_server, user_file, fmt):
    size = 1024**2
    create_image(user_file.path, fmt, size)

    nbd_server.image = user_file.path
    nbd_server.fmt = fmt
    nbd_server.start()

    data_length = 64 * 1024
    data_offset = 2 * data_length

    with nbd.open(nbd_server.url) as c:
        # Create 3 extents: zero, data, zero.
        c.write(data_offset, b"x" * data_length)

        # Unaligned part from first extent and last extent.
        extents = list(nbdutil.extents(c, data_offset - 1, data_length + 2))
        assert extents == [
            nbd.Extent(length=1, zero=True),
            nbd.Extent(length=data_length, zero=False),
            nbd.Extent(length=1, zero=True),
        ]

        # Unaligned part from second extent.
        extents = list(nbdutil.extents(c, data_offset + 1, data_length - 2))
        assert extents == [
            nbd.Extent(length=data_length - 2, zero=False),
        ]

        # Unaligned part from second and last extents.
        extents = list(nbdutil.extents(c, data_offset + 1, data_length))
        assert extents == [
            nbd.Extent(length=data_length - 1, zero=False),
            nbd.Extent(length=1, zero=True),
        ]


def test_base_allocation_many_extents(nbd_server, user_file):
    # Tested only with raw since qcow2 minimal extent is cluster size (64K),
    # and writing 1000 extents (62.5 MiB) will be too slow in the CI.

    # Extents must be multiple of file system block size.
    extent_length = os.statvfs(user_file.path).f_bsize

    # Use number which is not a multiple of our buffer capacity (1024 extents)
    # to ensure we read partial buffers correctly.
    extents_count = 2000

    size = extents_count * extent_length
    create_image(user_file.path, "raw", size)

    nbd_server.image = user_file.path
    nbd_server.fmt = "raw"
    nbd_server.start()

    with nbd.open(nbd_server.url) as c:
        # Write data to all even extents.
        data = b"x" * extent_length
        for i in range(0, size, extent_length * 2):
            c.write(i, data)

        extents = list(nbdutil.extents(c))

    assert len(extents) == extents_count

    for i, ext in enumerate(extents):
        assert ext.length == extent_length
        assert ext.zero == bool(i % 2)


def test_extents_reply_error(nbd_server, user_file):
    """
    The server SHOULD return NBD_EINVAL if it receives a NBD_CMD_BLOCK_STATUS
    request including one or more sectors beyond the size of the device.
    """
    size = 1024**2
    create_image(user_file.path, "raw", size)

    nbd_server.image = user_file.path
    nbd_server.fmt = "raw"
    nbd_server.start()

    with nbd.open(nbd_server.url) as c:
        with pytest.raises(nbd.ReplyError) as e:
            c.extents(0, size + 1)

        # Server should return this, qemu does.
        assert e.value.code == errno.EINVAL

        # The next request should succeed.
        assert c.read(4096, 1) == b"\0"


# Communicate with qemu builtin NBD server


@pytest.mark.parametrize("transport", ["unix", "tcp"])
@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_full_backup_handshake(tmpdir, fmt, transport):
    image = str(tmpdir.join("image"))
    subprocess.check_call(["qemu-img", "create", "-f", fmt, image, "1g"])

    if transport == "unix":
        sock = nbd.UnixAddress(tmpdir.join("sock"))
    else:
        sock = nbd.TCPAddress("localhost", testutil.random_tcp_port())

    with backup.full_backup(tmpdir, image, fmt, sock):
        with nbd.Client(sock, "sda") as c:
            # TODO: test transmission_flags?
            assert c.export_size == 1024**3
            assert c.minimum_block_size == 1
            assert c.preferred_block_size == 4096
            assert c.maximum_block_size == 32 * 1024**2
            assert c.base_allocation


@pytest.mark.parametrize("transport", ["unix", "tcp"])
@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_full_backup_single_image(tmpdir, fmt, transport):
    chunk_size = 1024**2
    disk_size = 5 * chunk_size

    # Create disk
    disk = str(tmpdir.join("disk." + fmt))
    subprocess.check_call([
        "qemu-img",
        "create",
        "-f", fmt,
        disk,
        str(disk_size),
    ])

    # Pupulate disk with data.
    with qemu_nbd.open(disk, fmt) as d:
        for i in range(0, disk_size, chunk_size):
            d.write(i, b"%d\n" % i)
        d.flush()

    if transport == "unix":
        sock = nbd.UnixAddress(tmpdir.join("sock"))
    else:
        sock = nbd.TCPAddress("localhost", testutil.random_tcp_port())

    # Start full backup and copy the data, veifying what we read.
    with backup.full_backup(tmpdir, disk, fmt, sock):
        with nbd.Client(sock, "sda") as c:
            log.debug("Backing up data with nbd client")
            for i in range(0, disk_size, chunk_size):
                data = c.read(i, chunk_size)
                assert data.startswith(b"%d\n\0" % i)


@pytest.mark.parametrize("transport", ["unix", "tcp"])
def test_full_backup_complete_chain(tmpdir, transport):
    depth = 3
    chunk_size = 1024**2
    disk_size = depth * chunk_size

    for i in range(depth):
        # Create disk based on previous one.
        disk = str(tmpdir.join("disk.%d" % i))
        cmd = ["qemu-img", "create", "-f", "qcow2"]

        if i > 0:
            cmd.append("-b")
            cmd.append("disk.%d" % (i - 1))

        cmd.append(disk)
        cmd.append(str(disk_size))

        subprocess.check_call(cmd)

        # This data can be read only from this disk.
        with qemu_nbd.open(disk, "qcow2") as d:
            d.write(i * chunk_size, b"%d\n" % i)
            d.flush()

    if transport == "unix":
        sock = nbd.UnixAddress(tmpdir.join("sock"))
    else:
        sock = nbd.TCPAddress("localhost", testutil.random_tcp_port())

    # Start full backup and copy the data, veifying what we read.
    with backup.full_backup(tmpdir, disk, "qcow2", sock):
        with nbd.Client(sock, "sda") as c:
            log.debug("Backing up data with nbd client")
            for i in range(depth):
                # Every chunk comes from different image.
                data = c.read(i * chunk_size, chunk_size)
                assert data.startswith(b"%d\n\0" % i)


def create_image(path, fmt, size):
    if fmt == "raw":
        # qemu-img allocates the first block on Fedora, but not on CentOS 8.0.
        # Allocate manually for consistent results.
        # TODO: Use qemu-img when we have CentOS 8.1 AV.
        with io.open(path, "wb") as f:
            f.truncate(size)
    else:
        subprocess.check_call(
            ["qemu-img", "create", "-f", "qcow2", path, str(size)])
