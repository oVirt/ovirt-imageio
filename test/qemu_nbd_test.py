# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import io
import os
import shutil
import struct
import tarfile
import tempfile
import urllib.parse

import pytest

from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import nbdutil
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import sockutil

from . import testutil
from . marks import flaky_in_ovirt_ci


@pytest.fixture
def tmpfs_dir():
    path = tempfile.mkdtemp(dir="/dev/shm")
    yield path
    shutil.rmtree(path)


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
    assert srv.url == urllib.parse.urlparse(url)


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


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_server_can_use_direct_io(tmpdir, nbd_server, fmt):
    disk = str(tmpdir.join("disk." + fmt))
    qemu_img.create(disk, fmt, size=1024**2)

    nbd_server.image = disk
    nbd_server.fmt = fmt
    nbd_server.cache = None
    nbd_server.aio = None

    # Starting probes the image and select cache="none" and aio="native"
    # if direct I/O can be used.
    nbd_server.start()
    assert nbd_server.cache == "none"
    assert nbd_server.aio == "native"


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_server_cannot_use_direct_io(tmpfs_dir, nbd_server, fmt):
    disk = os.path.join(tmpfs_dir, "disk." + fmt)
    qemu_img.create(disk, fmt, size=1024**2)

    nbd_server.image = disk
    nbd_server.fmt = fmt
    nbd_server.cache = None
    nbd_server.aio = None

    # Starting probes the image and selects cache="writeback" and
    # aio="threads" since direct I/O cannot be used with tmpfs file
    # system.
    nbd_server.start()
    assert nbd_server.cache == "writeback"
    assert nbd_server.aio == "threads"


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_server_keep_cache(tmpdir, nbd_server, fmt):
    disk = str(tmpdir.join("disk." + fmt))
    qemu_img.create(disk, fmt, size=1024**2)

    nbd_server.image = disk
    nbd_server.fmt = fmt
    nbd_server.cache = "writeback"
    nbd_server.aio = None

    # Staring the server selects aio="threads" since cache is not
    # "none".
    nbd_server.start()
    assert nbd_server.cache == "writeback"
    assert nbd_server.aio == "threads"


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_server_keep_aio(tmpdir, nbd_server, fmt):
    disk = str(tmpdir.join("disk." + fmt))
    qemu_img.create(disk, fmt, size=1024**2)

    nbd_server.image = disk
    nbd_server.fmt = fmt
    nbd_server.cache = None
    nbd_server.aio = "threads"

    # Staring the server probes the image and selects cache="none", but
    # keeps the requested aio option.
    nbd_server.start()
    assert nbd_server.cache == "none"
    assert nbd_server.aio == "threads"


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_server_keep_cache_and_aio(tmpdir, nbd_server, fmt):
    disk = str(tmpdir.join("disk." + fmt))
    qemu_img.create(disk, fmt, size=1024**2)

    nbd_server.image = disk
    nbd_server.fmt = fmt
    nbd_server.cache = "writeback"
    nbd_server.aio = "threads"

    # Staring the server keeps the requested options.
    nbd_server.start()
    assert nbd_server.cache == "writeback"
    assert nbd_server.aio == "threads"


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_ova(tmpdir, fmt):
    size = 1024**2
    offset = 64 * 1024
    disks = []

    # Created disks with unique content.
    for i in range(2):
        disk = str(tmpdir.join("disk{}.{}".format(i, fmt)))
        qemu_img.create(disk, fmt, size=size)
        with qemu_nbd.open(disk, fmt) as d:
            d.write(offset, disk.encode("utf-8"))
            d.flush()
        disks.append(disk)

    ova = str(tmpdir.join("vm.ova"))

    # Create a ova file.
    with tarfile.open(ova, "w") as tar:
        for disk in disks:
            tar.add(disk, arcname=os.path.basename(disk))

    # Read disks contents from the ova file.
    with tarfile.open(ova) as tar:
        for disk in disks:
            member = tar.getmember(os.path.basename(disk))
            with qemu_nbd.open(
                    ova,
                    fmt=fmt,
                    read_only=True,
                    offset=member.offset_data,
                    size=member.size) as d:
                assert d.export_size == size
                data = disk.encode("utf-8")
                assert d.read(offset, len(data)) == data


def test_ova_compressed_qcow2(tmpdir):
    size = 1024**2
    offset = 64 * 1024
    data = b"I can eat glass and it doesn't hurt me."

    tmp = str(tmpdir.join("disk.raw"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.seek(offset)
        f.write(data)

    disk = str(tmpdir.join("disk.qcow2"))
    qemu_img.convert(tmp, disk, "raw", "qcow2", compressed=True)

    ova = str(tmpdir.join("vm.ova"))

    # Create tar file with compressed qcow2 disk.
    with tarfile.open(ova, "w") as tar:
        tar.add(disk, arcname=os.path.basename(disk))

    # Read disk contents from the tar file.
    with tarfile.open(ova) as tar:
        member = tar.getmember(os.path.basename(disk))
        with qemu_nbd.open(
                ova,
                fmt="qcow2",
                read_only=True,
                offset=member.offset_data,
                size=member.size) as d:
            assert d.export_size == size
            assert d.read(offset, len(data)) == data


def test_run_unix(tmpdir):
    image = str(tmpdir.join("image"))
    sock = str(tmpdir.join("sock"))

    with io.open(image, "wb") as f:
        f.truncate(1024**2)

    addr = nbd.UnixAddress(sock)

    with qemu_nbd.run(image, "raw", addr):
        # The helper already waited for the NBD socket, not wait needed.
        assert sockutil.wait_for_socket(addr, 0.0)

    # The socket must be closed, no wait needed.
    assert not sockutil.wait_for_socket(addr, 0.0)


@flaky_in_ovirt_ci
def test_run_tcp(tmpfile):
    with io.open(tmpfile, "r+b") as f:
        f.truncate(1024**2)

    addr = nbd.TCPAddress("localhost", testutil.random_tcp_port())

    with qemu_nbd.run(tmpfile, "raw", addr):
        # The helper already waited for the NBD socket, not wait needed.
        assert sockutil.wait_for_socket(addr, 0.0)

    # The socket must be closed, no wait needed.
    assert not sockutil.wait_for_socket(addr, 0.0)


@pytest.mark.parametrize("options", [
    pytest.param({
        "cache": "none",
        "aio": "native",
        "discard": "unmap",
        "detect_zeroes": "unmap"
    }, id="qemu_nbd defaults"),
    pytest.param({
        "cache": None,
        "aio": None,
        "discard": None,
        "detect_zeroes": None
    }, id="qemu-nbd defaults"),
])
@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_options(tmpdir, fmt, options):
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
                **options), \
            qemu_nbd.run(
                dst, fmt, dst_addr,
                **options), \
            nbd.Client(src_addr) as src_client, \
            nbd.Client(dst_addr) as dst_client:

        nbdutil.copy(src_client, dst_client)

    qemu_img.compare(src, dst)


def test_backing_chain(tmpdir):
    size = 128 * 1024
    base = str(tmpdir.join("base.raw"))
    top = str(tmpdir.join("top.qcow2"))

    base_data = b"data from base".ljust(32, b"\0")

    # Add base image with some data.
    qemu_img.create(base, "raw", size=size)
    with qemu_nbd.open(base, "raw") as c:
        c.write(0, base_data)
        c.flush()

    # Add empty overlay.
    qemu_img.create(top, "qcow2", backing_file=base, backing_format="raw")

    top_addr = nbd.UnixAddress(str(tmpdir.join("sock")))

    # By default, we see data from base.
    with qemu_nbd.run(top, "qcow2", top_addr), \
            nbd.Client(top_addr) as c:
        assert c.read(0, 32) == base_data

    # With backing chain disabled, we see data only from top.
    with qemu_nbd.run(top, "qcow2", top_addr, backing_chain=False), \
            nbd.Client(top_addr) as c:
        assert c.read(0, 32) == b"\0" * 32


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


def test_dirty_bitmap(tmpdir):
    size = 1024**2

    # Create image with empty bitmap.
    img = str(tmpdir.join("img.qcow2"))
    qemu_img.create(img, "qcow2", size=size)
    qemu_img.bitmap_add(img, "b0")

    # Write data to image, modifying the bitmap.
    with qemu_nbd.open(img, "qcow2") as c:
        # This will allocate one cluster. By default bitmap granularity is also
        # one cluster, so this will make the first extent dirty.
        c.write(0, b"a")
        c.flush()

    # Read dirty extents.
    with qemu_nbd.open(img, "qcow2", read_only=True, bitmap="b0") as c:
        extents = c.extents(0, size)[nbd.QEMU_DIRTY_BITMAP + "b0"]

    bitmap = qemu_img.info(img)["format-specific"]["data"]["bitmaps"][0]

    assert extents == [
        nbd.Extent(length=bitmap["granularity"], flags=nbd.EXTENT_DIRTY),
        nbd.Extent(length=size - bitmap["granularity"], flags=0),
    ]


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
@pytest.mark.parametrize("detect_zeroes", ["on", "unmap"])
def test_detect_zeroes_enabled(tmpdir, fmt, detect_zeroes):
    size = 1024**2

    disk = str(tmpdir.join("disk." + fmt))
    qemu_img.create(disk, fmt, size=size)

    with qemu_nbd.open(disk, fmt, detect_zeroes=detect_zeroes) as c:
        # These zeroes should be detected.
        c.write(0, b"\0" * size)
        c.flush()
        extents = c.extents(0, size)

    assert extents["base:allocation"] == [
        nbd.Extent(length=1048576, flags=3),
    ]

    if fmt != "raw":
        assert extents["qemu:allocation-depth"] == [
            nbd.Extent(length=1048576, flags=0),
        ]


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
@pytest.mark.parametrize("detect_zeroes", ["off", None])
def test_detect_zeroes_disabled(tmpdir, fmt, detect_zeroes):
    size = 1024**2

    disk = str(tmpdir.join("disk." + fmt))
    qemu_img.create(disk, fmt, size=size)

    with qemu_nbd.open(disk, fmt, detect_zeroes=detect_zeroes) as c:
        # These zeroes should not be detected.
        c.write(0, b"\0" * size)
        c.flush()
        extents = c.extents(0, size)

    assert extents["base:allocation"] == [
        nbd.Extent(length=1048576, flags=0),
    ]

    if fmt != "raw":
        assert extents["qemu:allocation-depth"] == [
            nbd.Extent(length=1048576, flags=0),
        ]
