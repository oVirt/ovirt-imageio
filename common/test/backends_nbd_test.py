# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from contextlib import closing

import pytest
import userstorage

from ovirt_imageio_common import errors
from ovirt_imageio_common import qemu_img
from ovirt_imageio_common import util
from ovirt_imageio_common.backends import image
from ovirt_imageio_common.backends import nbd

from . import storage
from . marks import requires_python3

pytestmark = requires_python3

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
        assert b.sparse
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


def test_open_read_larger_buffer(nbd_server):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r") as b:
        # buffer larger than remaining data
        buffer_length = 10
        data_length = 5
        with closing(util.aligned_buffer(buffer_length)) as buf:
            b.seek(b._client.export_size - data_length)
            assert b.readinto(buf) == data_length
        b.flush()


def test_open_readonly(nbd_server):
    nbd_server.read_only = True
    nbd_server.start()
    with nbd.open(nbd_server.url, "r") as b:
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


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_middle(nbd_server, sparse):
    nbd_server.start()
    with nbd.open(nbd_server.url, "r+", sparse=sparse) as b:
        # nbd backend is always sparse.
        assert b.sparse

        b.write(b"xxxxxxxxxxxx")
        b.seek(4)
        assert b.zero(4) == 4

        with closing(util.aligned_buffer(12)) as buf:
            b.seek(0)
            assert b.readinto(buf) == 12
            assert buf[:] == b"xxxx\x00\x00\x00\x00xxxx"


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
    with nbd.open(nbd_server.url, "r") as b:
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

        assert list(b.extents()) == [
            image.ZeroExtent(0, len(data), False),
            image.ZeroExtent(len(data), 5 * 1024**3 - len(data), True),
            image.ZeroExtent(5 * 1024**3, len(data), False),
            image.ZeroExtent(
                5 * 1024**3 + len(data), 1024**3 - len(data), True),
        ]


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_extents_dirty_not_availabe(nbd_server, fmt):
    qemu_img.create(nbd_server.image, fmt, 65536)
    nbd_server.fmt = fmt
    nbd_server.start()

    with nbd.open(nbd_server.url, "r+", dirty=True) as b:
        with pytest.raises(errors.UnsupportedOperation):
            list(b.extents(context="dirty"))
