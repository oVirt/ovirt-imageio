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

from ovirt_imageio_common import util
from ovirt_imageio_common.backends import nbd


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
