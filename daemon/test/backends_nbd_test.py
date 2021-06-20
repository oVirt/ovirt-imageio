# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import os
from contextlib import closing

import pytest
import userstorage

from ovirt_imageio._internal import errors
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import util
from ovirt_imageio._internal.backends import image
from ovirt_imageio._internal.backends import nbd

from . import storage

BACKENDS = userstorage.load_config("../storage.py").BACKENDS


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


def test_debugging_interface(nbd_server):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r+") as b:
        assert b.readable()
        assert b.writable()
        assert not b.sparse
        assert b.name == "nbd"


def test_open_read_write(nbd_server):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r+") as b:
        assert b.readable()
        assert b.writable()

        data = b"data"
        b.write(data)
        assert b.tell() == len(data)

        b.zero(4)
        size = len(data) + 4
        assert b.tell() == size

        with closing(util.aligned_buffer(size)) as buf:
            b.seek(0)
            assert b.readinto(buf) == size
            assert buf[:] == data + b"\0" * 4
        b.flush()


def test_open_readonly(nbd_server):
    nbd_server.read_only = True
    nbd_server.start()
    with nbd.open(nbd_server.url) as b:
        assert b.readable()
        assert not b.writable()

        with pytest.raises(IOError):
            b.write(b"data")
        assert b.tell() == 0

        with pytest.raises(IOError):
            b.zero(4)
        assert b.tell() == 0

        with closing(util.aligned_buffer(100)) as buf:
            buf.write(b"x" * 100)
            assert b.readinto(buf) == len(buf)
            assert buf[:] == b"\0" * len(buf)

        b.flush()


def test_open_writeonly(nbd_server):
    nbd_server.start()
    with nbd.open(nbd_server.url, "w") as b:
        assert not b.readable()
        assert b.writable()

        data = b"data"
        b.write(data)
        assert b.tell() == len(data)

        with pytest.raises(IOError):
            with closing(util.aligned_buffer(100)) as buf:
                b.readinto(buf)

        b.flush()


def test_invalid_mode(nbd_server):
    nbd_server.start()
    with pytest.raises(ValueError):
        nbd.open(nbd_server.url, "invalid")


@pytest.mark.parametrize("img_size,buf_size", [
    pytest.param(4096, 4096, id="full"),
    pytest.param(4096, 8192, id="short-aligned"),
    pytest.param(4097, 8192, id="short-unaligned"),
])
def test_readinto(nbd_server, img_size, buf_size):
    with open(nbd_server.image, "wb") as f:
        f.write(b"x" * img_size)

    nbd_server.start()

    buf = bytearray(buf_size)
    with nbd.open(nbd_server.url, "r+") as f:
        # When image size is not aligned to 512 bytes, the last read is
        # extended to the next multiple of 512.
        effective_size = f.size()

        n = f.readinto(buf)
        assert n == effective_size
        assert f.tell() == effective_size
        assert buf[:img_size] == b"x" * img_size
        assert buf[img_size:] == b"\0" * (buf_size - img_size)


@pytest.mark.parametrize("end_offset", [
    pytest.param(0, id="at-end"),
    pytest.param(1, id="after-end"),
])
def test_readinto_end(nbd_server, end_offset):
    nbd_server.start()
    buf = bytearray(512)
    with nbd.open(nbd_server.url, "r+") as f:
        offset = f.size() + end_offset
        f.seek(offset)
        n = f.readinto(buf)
        assert n == 0
        assert f.tell() == offset


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_middle(nbd_server, sparse):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r+", sparse=sparse) as b:
        b.write(b"xxxxxxxxxxxx")
        b.seek(4)
        assert b.zero(4) == 4

        with closing(util.aligned_buffer(12)) as buf:
            b.seek(0)
            assert b.readinto(buf) == 12
            assert buf[:] == b"xxxx\x00\x00\x00\x00xxxx"


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_sparse(nbd_server, user_file, sparse):
    size = 10 * 1024**2
    qemu_img.create(user_file.path, "raw", size=size)
    nbd_server.image = user_file.path
    nbd_server.start()

    with nbd.open(nbd_server.url, "r+", sparse=sparse) as b:
        b.zero(b.size())
        b.flush()
        actual_size = os.stat(user_file.path).st_blocks * 512
        assert actual_size == 0 if sparse else b.size()


def test_close(nbd_server):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r+") as b:
        pass

    # Closing twice does not do anything.
    b.close()

    # But other operations should fail now with:
    #     socket.error: Bad file descriptor
    with pytest.raises(IOError):
        b.write("more")
    with pytest.raises(IOError):
        with closing(util.aligned_buffer(100)) as buf:
            b.readinto(buf)


def test_context_manager(nbd_server):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r+") as b:
        b.write(b"data")
    with pytest.raises(IOError):
        b.write("more")


def test_dirty(nbd_server):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r+") as b:
        # backend created clean
        assert not b.dirty

        # write and zero dirty the backend
        b.write(b"01234")
        assert b.dirty

        b.flush()
        assert not b.dirty

        b.zero(5)
        assert b.dirty

        b.flush()
        assert not b.dirty

        # readinto, seek do not affect dirty.
        b.seek(0)
        assert not b.dirty

        with closing(util.aligned_buffer(10)) as buf:
            b.readinto(buf)
        assert not b.dirty


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_size(nbd_server, fmt):
    size = 150 * 1024**2
    nbd_server.fmt = fmt
    qemu_img.create(nbd_server.image, fmt, size=size)
    nbd_server.start()
    with nbd.open(nbd_server.url) as b:
        assert b.size() == size


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_extents_zero(nbd_server, user_file, fmt):
    size = 6 * 1024**3
    qemu_img.create(user_file.path, fmt, size=size)

    nbd_server.image = user_file.path
    nbd_server.fmt = fmt
    nbd_server.start()

    with nbd.open(nbd_server.url, "r+") as b:
        # qcow2 extents resolution is cluster size.
        data = b"x" * 64 * 1024
        b.write(data)

        # The second extent length is bigger than NBD maximum length, testing
        # that our extent length is not limited by NBD limits. The backend
        # sends multiple block status commands and merge the returned extents.
        b.seek(5 * 1024**3)
        b.write(data)

        # Holes can be reported only for qcow2 images.
        hole = fmt == "qcow2"

        assert list(b.extents()) == [
            image.ZeroExtent(0, len(data), False, False),
            image.ZeroExtent(len(data), 5 * 1024**3 - len(data), True, hole),
            image.ZeroExtent(5 * 1024**3, len(data), False, False),
            image.ZeroExtent(
                5 * 1024**3 + len(data), 1024**3 - len(data), True, hole),
        ]


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_extents_dirty_not_availabe(nbd_server, fmt):
    qemu_img.create(nbd_server.image, fmt, 65536)
    nbd_server.fmt = fmt
    nbd_server.start()

    with nbd.open(nbd_server.url, "r+", dirty=True) as b:
        with pytest.raises(errors.UnsupportedOperation):
            list(b.extents(context="dirty"))


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_clone(nbd_server, fmt):
    qemu_img.create(nbd_server.image, fmt, 65536)
    nbd_server.fmt = fmt
    nbd_server.start()

    with nbd.open(nbd_server.url, "r+") as a, \
            a.clone() as b:
        # Backends are indentical when created.
        assert a.size() == b.size()
        assert a.tell() == b.tell()
        assert a.block_size == b.block_size

        # Modifying one backend does not change the other.
        a.write(b"x" * 4096)
        assert b.tell() == 0

        # Both backends expoose the same content.
        buf = bytearray(4096)
        b.readinto(buf)
        assert buf == b"x" * 4096
