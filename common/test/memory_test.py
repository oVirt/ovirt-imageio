# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest

from ovirt_imageio_common.backends import memory


def test_open_read_write():
    m = memory.open("r+")
    assert m.readable()
    assert m.writable()

    data = b"data"
    m.write(data)
    assert m.tell() == len(data)

    m.zero(4)
    size = len(data) + 4
    assert m.tell() == size
    b = bytearray(size)
    m.seek(0)
    assert m.readinto(b) == size
    assert b == data + b"\0" * 4


def test_open_readonly():
    m = memory.open("r")
    assert m.readable()
    assert not m.writable()

    with pytest.raises(IOError):
        m.write(b"data")
    with pytest.raises(IOError):
        m.zero(4)
    assert m.tell() == 0
    b = bytearray(b"before")
    assert m.readinto(b) == 0
    assert b == b"before"
    m.flush()


def test_open_writeonly():
    m = memory.open("w")
    assert not m.readable()
    assert m.writable()

    data = b"data"
    m.write(data)
    assert m.tell() == len(data)
    with pytest.raises(IOError):
        m.readinto(bytearray(10))
    m.flush()


def test_invalid_mode():
    with pytest.raises(ValueError):
        memory.open("invalid")


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_middle(sparse):
    m = memory.open("r+", sparse=sparse)
    m.write(b"xxxxxxxxxxxx")
    m.seek(4)
    n = m.zero(4)
    assert n == 4
    b = bytearray(13)
    m.seek(0)
    assert m.readinto(b) == 12
    assert b[:12] == b"xxxx\x00\x00\x00\x00xxxx"


def test_close():
    m = memory.open("r+")
    m.close()
    # All operations should fail now with:
    #     ValueError: I/O operation on closed file
    with pytest.raises(ValueError):
        m.write("more")
    with pytest.raises(ValueError):
        m.readinto(bytearray(10))


def test_context_manager():
    with memory.open("r+") as m:
        m.write(b"data")
    with pytest.raises(ValueError):
        m.write("more")


def test_close_error():

    def close():
        raise IOError("backend error")

    with pytest.raises(IOError):
        with memory.open("r+") as m:
            m.close = close


def test_propagate_user_error():

    class UserError(Exception):
        pass

    def close():
        raise IOError("backend error")

    with pytest.raises(UserError):
        with memory.open("r+") as m:
            m.close = close
            raise UserError("user error")


def test_create_with_bytes():
    m = memory.Backend("r", b"data")
    assert m.readable()
    assert not m.writable()

    b = bytearray(5)
    assert m.readinto(b) == 4
    assert b[:] == b"data\0"
