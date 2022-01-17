# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import io
import logging
import os

import pytest
import userstorage

from urllib.parse import urlparse

from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import nbdutil
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd

from . import backup
from . import ci
from . import distro
from . import storage
from . import testutil

from . marks import (
    flaky_in_ovirt_ci,
    requires_ipv6,
)

BACKENDS = userstorage.load_config("storage.py").BACKENDS

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


@pytest.fixture(params=[
    "unix",
    pytest.param("tcp", marks=flaky_in_ovirt_ci),
])
def nbd_sock(request, tmpdir):
    if request.param == "unix":
        return nbd.UnixAddress(tmpdir.join("sock"))
    else:
        return nbd.TCPAddress("localhost", testutil.random_tcp_port())


# Addresses

@pytest.mark.parametrize("addr,export,url", [
    # Note: We get Unicode when parsing ticket JSON.
    (nbd.UnixAddress("/sock"), None, "nbd:unix:/sock"),
    (nbd.UnixAddress("/sock"), "", "nbd:unix:/sock"),
    (nbd.UnixAddress("/sock"), "sda", "nbd:unix:/sock:exportname=sda"),
    (nbd.TCPAddress("host", 10900), None, "nbd:host:10900"),
    (nbd.TCPAddress("host", 10900), "", "nbd:host:10900"),
    (nbd.TCPAddress("host", 10900), "sdb", "nbd:host:10900:exportname=sdb"),
])
def test_url(addr, export, url):
    assert addr.url(export) == url


@pytest.mark.parametrize("host,port", [
    ("localhost", "42"),
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
    pytest.param("\u05d0", id="unicode"),
])
def test_handshake(tmpdir, export, fmt):
    image = str(tmpdir.join("image"))
    create_image(image, fmt, 1024**3)
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
            # Both available in in current qemu-nbd (>= 5.2.0).
            assert c.has_base_allocation
            assert c.has_allocation_depth == (fmt != "raw")


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
    create_image(image, "qcow2", 1024**3)

    with qemu_nbd.run(image, "qcow2", sock):
        with nbd.Client(sock) as c:
            c.write(offset, data)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, len(data)) == data


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_zero(tmpdir, fmt):
    size = 2 * 1024**2
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    create_image(image, fmt, size)

    with qemu_nbd.run(image, fmt, sock):
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


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_zero_max_block_size(tmpdir, fmt):
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    create_image(image, fmt, 1024**3)

    with qemu_nbd.run(image, fmt, sock):
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


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_zero_min_block_size(tmpdir, fmt):
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = nbd.UnixAddress(tmpdir.join("sock"))
    create_image(image, fmt, 1024**3)

    with qemu_nbd.run(image, fmt, sock):
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


def test_zero_allocation(user_file):
    # Create sparse image.
    with open(user_file.path, "w") as f:
        f.truncate(2 * 1024**2)

    with qemu_nbd.open(user_file.path, "raw") as c:
        # Zero with punch_hole=False allocates space.
        c.zero(0, c.export_size, punch_hole=False)
        c.flush()
        assert os.stat(user_file.path).st_blocks * 512 == c.export_size

        # Default zero deallocates space (depends on qemu-nbd default).
        c.zero(0, c.export_size)
        c.flush()
        assert os.stat(user_file.path).st_blocks == 0


@pytest.mark.parametrize("url,export", [
    # Note: We get Unicode URL when parsing ticket JSON.
    ("nbd:unix:/path", ""),
    ("nbd:unix:/path:exportname=", ""),
    ("nbd:unix:/path:exportname=sda", "sda"),
    ("nbd:unix:/path:exportname=/sda", "/sda"),
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


@flaky_in_ovirt_ci
@pytest.mark.parametrize("url_template,host,export", [
    # Note: We get Unicode URL when parsing ticket JSON.
    # DNS name
    ("nbd:localhost:{port}", "localhost", ""),
    ("nbd:localhost:{port}:exportname=", "localhost", ""),
    ("nbd:localhost:{port}:exportname=sda", "localhost", "sda"),
    ("nbd:localhost:{port}:exportname=/sda", "localhost", "/sda"),
    ("nbd://localhost:{port}", "localhost", ""),
    ("nbd://localhost:{port}/", "localhost", ""),
    ("nbd://localhost:{port}/sda", "localhost", "sda"),
    ("nbd://localhost:{port}//sda", "localhost", "/sda"),
    # IPv4
    ("nbd://127.0.0.1:{port}", "127.0.0.1", ""),
    ("nbd:127.0.0.1:{port}", "127.0.0.1", ""),
    # IPv6
    pytest.param("nbd://[::1]:{port}", "[::1]", "", marks=requires_ipv6),
    pytest.param("nbd:[::1]:{port}", "[::1]", "", marks=requires_ipv6),
])
def test_open_tcp(tmpdir, url_template, host, export):
    image = str(tmpdir.join("image"))
    with open(image, "wb") as f:
        f.truncate(1024**3)

    sock = nbd.TCPAddress(host, testutil.random_tcp_port())
    url = url_template.format(port=sock.port)

    log.debug("Trying url=%r export=%r", url, export)
    with qemu_nbd.run(image, "raw", sock, export_name=export):
        with nbd.open(urlparse(url)) as c:
            assert c.export_size == 1024**3


FMT_HOLE_FLAGS_PARAMS = [
    # In qemu-nbd 5.2.0 we get only the zero bit.
    ["raw", nbd.STATE_ZERO],
    ["qcow2", nbd.STATE_ZERO | nbd.STATE_HOLE | nbd.EXTENT_BACKING],
]

# Since qemu-nbd 6.0.0 we get also the hole bit, but we don't get the backing
# bit, so unallocated areas in raw images are not considered as holes.
if qemu_nbd.version() >= (6, 0, 0):
    FMT_HOLE_FLAGS_PARAMS[0][1] |= nbd.STATE_HOLE


@pytest.mark.parametrize("fmt,hole_flags", FMT_HOLE_FLAGS_PARAMS)
def test_base_allocation_empty(nbd_server, user_file, fmt, hole_flags):
    size = 1024**3
    create_image(user_file.path, fmt, size)

    nbd_server.image = user_file.path
    nbd_server.fmt = fmt
    nbd_server.start()

    with nbd.open(nbd_server.url) as c:
        # Entire image.
        extents = list(nbdutil.extents(c))
        assert extents == [nbd.Extent(length=size, flags=hole_flags)]

        # First block.
        extents = list(nbdutil.extents(c, length=4096))
        assert extents == [nbd.Extent(length=4096, flags=hole_flags)]

        # Last block.
        extents = list(nbdutil.extents(c, offset=size-4096, length=4096))
        assert extents == [nbd.Extent(length=4096, flags=hole_flags)]

        # Some block.
        extents = list(nbdutil.extents(c, offset=4096, length=4096))
        assert extents == [nbd.Extent(length=4096, flags=hole_flags)]

        # Unaligned start.
        extents = list(nbdutil.extents(c, offset=4096 - 1, length=4096 + 1))
        assert extents == [nbd.Extent(length=4096 + 1, flags=hole_flags)]

        # Unaligned end.
        extents = list(nbdutil.extents(c, offset=4096, length=4096 + 1))
        assert extents == [nbd.Extent(length=4096 + 1, flags=hole_flags)]

        # Unaligned start and end.
        extents = list(nbdutil.extents(c, offset=4096 - 1, length=4096 + 2))
        assert extents == [nbd.Extent(length=4096 + 2, flags=hole_flags)]


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
        extents = list(nbdutil.extents(c))
        assert extents == [nbd.Extent(length=size, flags=0)]

        # First block.
        extents = list(nbdutil.extents(c, length=4096))
        assert extents == [nbd.Extent(length=4096, flags=0)]

        # Last block.
        extents = list(nbdutil.extents(c, offset=size-4096, length=4096))
        assert extents == [nbd.Extent(length=4096, flags=0)]

        # Some block.
        extents = list(nbdutil.extents(c, offset=4096, length=4096))
        assert extents == [nbd.Extent(length=4096, flags=0)]

        # Unaligned start.
        extents = list(nbdutil.extents(c, offset=4096 - 1, length=4096 + 1))
        assert extents == [nbd.Extent(length=4096 + 1, flags=0)]

        # Unaligned end.
        extents = list(nbdutil.extents(c, offset=4096, length=4096 + 1))
        assert extents == [nbd.Extent(length=4096 + 1, flags=0)]

        # Unaligned start and end.
        extents = list(nbdutil.extents(c, offset=4096 - 1, length=4096 + 2))
        assert extents == [nbd.Extent(length=4096 + 2, flags=0)]


@pytest.mark.parametrize("fmt,hole_flags", FMT_HOLE_FLAGS_PARAMS)
def test_base_allocation_some_data(nbd_server, user_file, fmt, hole_flags):
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
        nbd.Extent(length=data_length, flags=0),
        nbd.Extent(length=zero_length, flags=hole_flags),
        nbd.Extent(length=data_length, flags=0),
        nbd.Extent(length=zero_length, flags=hole_flags),
    ]


@pytest.mark.parametrize("fmt,hole_flags", FMT_HOLE_FLAGS_PARAMS)
def test_base_allocation_some_data_unaligned(
        nbd_server, user_file, fmt, hole_flags):
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
            nbd.Extent(length=1, flags=hole_flags),
            nbd.Extent(length=data_length, flags=0),
            nbd.Extent(length=1, flags=hole_flags),
        ]

        # Unaligned part from second extent.
        extents = list(nbdutil.extents(c, data_offset + 1, data_length - 2))
        assert extents == [
            nbd.Extent(length=data_length - 2, flags=0),
        ]

        # Unaligned part from second and last extents.
        extents = list(nbdutil.extents(c, data_offset + 1, data_length))
        assert extents == [
            nbd.Extent(length=data_length - 1, flags=0),
            nbd.Extent(length=1, flags=hole_flags),
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


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_full_backup_handshake(tmpdir, fmt, nbd_sock):
    image = str(tmpdir.join("image"))
    create_image(image, fmt, 1024**3)

    with backup.full_backup(tmpdir, image, fmt, nbd_sock):
        with nbd.Client(nbd_sock, "sda") as c:
            # TODO: test transmission_flags?
            assert c.export_size == 1024**3
            assert c.minimum_block_size == 1
            assert c.preferred_block_size == 4096
            assert c.maximum_block_size == 32 * 1024**2
            # Both available in in current qemu (>= 5.2.0).
            assert c.has_base_allocation
            assert c.has_allocation_depth


@pytest.mark.parametrize("fmt", [
    pytest.param(
        "raw",
        marks=pytest.mark.xfail(
            distro.is_centos() and ci.is_ovirt(),
            reason="unaligned write fails on el8/oVirt CI")
    ),
    pytest.param("qcow2"),
])
def test_full_backup_single_image(tmpdir, user_file, fmt, nbd_sock):
    chunk_size = 1024**3
    disk_size = 5 * chunk_size

    # Create disk
    create_image(user_file.path, fmt, disk_size)

    # Pupulate disk with data.
    with qemu_nbd.open(user_file.path, fmt) as d:
        for offset in range(0, disk_size, chunk_size):
            d.write(offset, b"%d\n" % offset)
        d.flush()

    checkpoint = "check1" if fmt == "qcow2" else None

    # Start full backup and copy the data, veifying what we read.
    with backup.full_backup(
            tmpdir, user_file.path, fmt, nbd_sock, checkpoint=checkpoint):
        verify_full_backup(nbd_sock, "sda")

    if checkpoint:
        bitmaps = list_bitmaps(user_file.path)
        assert len(bitmaps) == 1
        assert bitmaps[0]["name"] == checkpoint


@pytest.mark.parametrize("checkpoint", [
    pytest.param(None, id="no-checkpoint"),
    pytest.param("check1", id="with-checkpoint"),
])
def test_full_backup_complete_chain(tmpdir, nbd_sock, checkpoint):
    depth = 3
    chunk_size = 1024**2
    disk_size = depth * chunk_size

    for i in range(depth):
        # Create disk based on previous one.
        disk = str(tmpdir.join("disk.{}".format(i)))
        if i == 0:
            qemu_img.create(disk, "qcow2", size=disk_size)
        else:
            qemu_img.create(
                disk,
                "qcow2",
                backing_file="disk.{}".format(i - 1),
                backing_format="qcow2")

        # This data can be read only from this disk.
        with qemu_nbd.open(disk, "qcow2") as d:
            offset = i * chunk_size
            d.write(offset, b"%d\n" % offset)
            d.flush()

    # Start full backup and copy the data, veifying what we read.
    with backup.full_backup(
            tmpdir, disk, "qcow2", nbd_sock, checkpoint=checkpoint):
        verify_full_backup(nbd_sock, "sda")

    if checkpoint:
        bitmaps = list_bitmaps(disk)
        assert len(bitmaps) == 1
        assert bitmaps[0]["name"] == checkpoint


def list_bitmaps(image):
    info = qemu_img.info(image)
    return info["format-specific"]["data"]["bitmaps"]


def verify_full_backup(sock, export_name):
    with nbd.Client(sock, export_name) as c:
        log.debug("Backing up data with nbd client")
        offset = 0
        for ext in nbdutil.extents(c):
            if not ext.zero:
                expected = b"%d\n" % offset
                data = c.read(offset, len(expected))
                assert data == expected
            offset += ext.length


def test_extent_base_allocation():
    # Allocated area with data.
    data = nbd.Extent.pack(4096, 0)
    ext = nbd.Extent.unpack(data)
    assert not ext.zero
    assert not ext.hole
    assert ext.flags == 0

    # Allocated area that reads as zero.
    data = nbd.Extent.pack(4096, nbd.STATE_ZERO)
    ext = nbd.Extent.unpack(data)
    assert ext.zero
    assert not ext.hole
    assert ext.flags == nbd.STATE_ZERO

    # Unallocated area that reads as zero.
    data = nbd.Extent.pack(4096, nbd.STATE_ZERO | nbd.STATE_HOLE)
    ext = nbd.Extent.unpack(data)
    assert ext.zero
    # Hole can be detected only when using depth data with qcow2 image.
    assert not ext.hole
    assert ext.flags == nbd.STATE_ZERO | nbd.STATE_HOLE

    # Ignore unexpected bits
    data = nbd.Extent.pack(4096, 0xffff)
    ext = nbd.Extent.unpack(data)
    assert ext.zero
    assert not ext.hole
    assert not ext.dirty
    assert ext.flags == nbd.STATE_HOLE | nbd.STATE_ZERO


def test_extent_dirty_bitmap():
    # Clean area.
    data = nbd.Extent.pack(4096, 0)
    ext = nbd.Extent.unpack(data, nbd.Extent.DIRTY)
    assert not ext.dirty
    assert ext.flags == 0

    # Dirty area.
    data = nbd.Extent.pack(4096, nbd.STATE_DIRTY)
    ext = nbd.Extent.unpack(data, nbd.Extent.DIRTY)
    assert ext.dirty
    # nbd.STATE_DIRTY is mapped to nbd.EXTENT_DIRTY to allow merging allocation
    # and dirty bitmap info in same extent.
    assert ext.flags == nbd.EXTENT_DIRTY

    # Ignore unexpected bits
    data = nbd.Extent.pack(4096, 0xffff)
    ext = nbd.Extent.unpack(data, nbd.Extent.DIRTY)
    assert not ext.zero
    assert not ext.hole
    assert ext.dirty
    assert ext.flags == nbd.EXTENT_DIRTY


def test_extent_depth():
    # Unallocated extent.
    depth_data = nbd.Extent.pack(65536, 0)
    ext = nbd.Extent.unpack(depth_data, nbd.Extent.DEPTH)
    assert ext.length == 65536
    assert not ext.zero
    # Hole status is determined only by the EXTENT_BACKING bit.
    assert ext.hole
    assert not ext.dirty
    assert ext.flags == nbd.EXTENT_BACKING

    # Allocated extents from top layer.
    data = nbd.Extent.pack(4096, 1)
    ext = nbd.Extent.unpack(data, nbd.Extent.DEPTH)
    assert not ext.zero
    assert not ext.hole
    assert not ext.dirty
    assert ext.flags == 0

    # Allocated extents from top backing file.
    data = nbd.Extent.pack(4096, 2)
    ext = nbd.Extent.unpack(data, nbd.Extent.DEPTH)
    assert not ext.zero
    assert not ext.hole
    assert not ext.dirty
    assert ext.flags == 0


def test_extent_compare():
    assert nbd.Extent(4096, 0) == nbd.Extent(4096, 0)
    assert nbd.Extent(4096, 0) != nbd.Extent(4096, nbd.STATE_ZERO)


def test_extent_merge_clean_hole():
    alloc_data = nbd.Extent.pack(4096, nbd.STATE_ZERO | nbd.STATE_HOLE)
    alloc = nbd.Extent.unpack(alloc_data)

    dirty_data = nbd.Extent.pack(4096, 0)
    dirty = nbd.Extent.unpack(dirty_data, nbd.Extent.DIRTY)

    merged = nbd.Extent(4096, alloc.flags | dirty.flags)

    assert merged.length == 4096
    assert merged.zero is True
    # Hole can be detected only when using depth data with qcow2 image.
    assert merged.hole is False
    assert not merged.dirty


def test_extent_merge_dirty_data():
    alloc_data = nbd.Extent.pack(4096, 0)
    alloc = nbd.Extent.unpack(alloc_data, nbd.Extent.DIRTY)

    dirty_data = nbd.Extent.pack(4096, nbd.STATE_DIRTY)
    dirty = nbd.Extent.unpack(dirty_data, nbd.Extent.DIRTY)

    merged = nbd.Extent(4096, alloc.flags | dirty.flags)

    assert merged.length == 4096
    assert not merged.zero
    assert not merged.hole
    assert merged.dirty


def test_extent_merge_qcow2_unallocated_cluster():
    alloc_data = nbd.Extent.pack(65536, nbd.STATE_ZERO | nbd.STATE_HOLE)
    alloc = nbd.Extent.unpack(alloc_data)

    depth_data = nbd.Extent.pack(65536, 0)
    depth = nbd.Extent.unpack(depth_data, nbd.Extent.DEPTH)

    merged = nbd.Extent(65536, alloc.flags | depth.flags)

    assert merged.length == 65536
    assert merged.zero is True
    assert merged.hole is True


def test_extent_merge_qcow2_zero_cluster():
    alloc_data = nbd.Extent.pack(65536, nbd.STATE_ZERO | nbd.STATE_HOLE)
    alloc = nbd.Extent.unpack(alloc_data)

    depth_data = nbd.Extent.pack(65536, 1)
    depth = nbd.Extent.unpack(depth_data, nbd.Extent.DEPTH)

    merged = nbd.Extent(65536, alloc.flags | depth.flags)

    assert merged.length == 65536
    assert merged.zero is True
    assert merged.hole is False


def test_extent_merge_qcow2_data_cluster():
    alloc_data = nbd.Extent.pack(65536, 0)
    alloc = nbd.Extent.unpack(alloc_data)

    depth_data = nbd.Extent.pack(65536, 1)
    depth = nbd.Extent.unpack(depth_data, nbd.Extent.DEPTH)

    merged = nbd.Extent(65536, alloc.flags | depth.flags)

    assert merged.length == 65536
    assert merged.zero is False
    assert merged.hole is False


def test_reply_error_structured():
    s = str(nbd.ReplyError(28, "writing to file failed"))
    assert s == "Writing to file failed: [Error 28] No space left on device"


def test_reply_error_simple():
    s = str(nbd.ReplyError(1, "Simple reply failed"))
    assert s == "Simple reply failed: [Error 1] Operation not permitted"


def test_reply_error_unknown_code():
    s = str(nbd.ReplyError(-1, "bad error"))
    assert s == "Bad error: [Error -1] Unknown error -1"


def test_reply_error_empty_message():
    s = str(nbd.ReplyError(5, ""))
    assert s == "Server error: [Error 5] Input/output error"


def create_image(path, fmt, size):
    if fmt == "raw":
        # qemu-img allocates the first block on Fedora, but not on CentOS 8.0.
        # Allocate manually for consistent results.
        # TODO: Use qemu-img when we have CentOS 8.1 AV.
        with io.open(path, "wb") as f:
            f.truncate(size)
    else:
        qemu_img.create(path, "qcow2", size=size)
