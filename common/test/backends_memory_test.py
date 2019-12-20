# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
from six.moves.urllib_parse import urlparse
import pytest

from ovirt_imageio_common.backends import image
from ovirt_imageio_common.backends import memory


def test_debugging_interface(tmpurl):
    with memory.open(urlparse("memory:"), "r+") as m:
        assert m.readable()
        assert m.writable()
        assert not m.sparse
        assert m.name == "memory"


def test_open_read_write():
    m = memory.Backend("r+")
    assert m.readable()
    assert m.writable()
    assert not m.sparse

    data = b"data"
    m.write(data)
    assert m.tell() == len(data)

    m.zero(4)
    size = len(data) + 4
    assert m.tell() == size

    content = data + b"\0" * 4
    b = bytearray(size)
    m.seek(0)
    assert m.readinto(b) == size
    assert b == content

    dst = io.BytesIO()
    m.seek(0)
    assert m.write_to(dst, 8) == 8
    assert dst.getvalue() == content


def test_open_readonly():
    m = memory.Backend("r")
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

    dst = io.BytesIO()
    m.seek(0)
    assert m.write_to(dst, 0) == 0
    assert dst.getvalue() == b""
    m.flush()


def test_open_writeonly():
    m = memory.Backend("w")
    assert not m.readable()
    assert m.writable()

    data = b"data"
    m.write(data)
    assert m.tell() == len(data)
    with pytest.raises(IOError):
        m.readinto(bytearray(10))
    m.seek(0)
    with pytest.raises(IOError):
        m.write_to(io.BytesIO(), 10)
    m.flush()


def test_invalid_mode():
    with pytest.raises(ValueError):
        memory.Backend("invalid")


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_middle(sparse):
    m = memory.open(urlparse("memory:"), "r+", sparse=sparse)
    assert not m.sparse
    m.write(b"xxxxxxxxxxxx")
    m.seek(4)
    n = m.zero(4)
    assert n == 4
    b = bytearray(13)
    m.seek(0)
    assert m.readinto(b) == 12
    assert b[:12] == b"xxxx\x00\x00\x00\x00xxxx"


def test_close():
    m = memory.Backend("r+")
    m.close()
    # All operations should fail now with:
    #     ValueError: I/O operation on closed file
    with pytest.raises(ValueError):
        m.write("more")
    with pytest.raises(ValueError):
        m.readinto(bytearray(10))


def test_context_manager():
    with memory.Backend("r+") as m:
        m.write(b"data")
    with pytest.raises(ValueError):
        m.write("more")


def test_close_error():

    def close():
        raise IOError("backend error")

    with pytest.raises(IOError):
        with memory.Backend("r+") as m:
            m.close = close


def test_propagate_user_error():

    class UserError(Exception):
        pass

    def close():
        raise IOError("backend error")

    with pytest.raises(UserError):
        with memory.Backend("r+") as m:
            m.close = close
            raise UserError("user error")


def test_create_with_bytes():
    m = memory.Backend("r", b"data")
    assert m.readable()
    assert not m.writable()

    b = bytearray(5)
    assert m.readinto(b) == 4
    assert b[:] == b"data\0"


def test_dirty():
    # backend created clean
    m = memory.Backend("r+", b"data")
    assert not m.dirty

    # write ans zero dirty the backend
    m.write(b"01234")
    assert m.dirty
    m.flush()
    assert not m.dirty
    m.zero(5)
    assert m.dirty
    m.flush()
    assert not m.dirty

    # readinto, seek do not affect dirty.
    b = bytearray(10)
    m.seek(0)
    assert not m.dirty
    m.readinto(b)
    assert not m.dirty


def test_size():
    m = memory.Backend("r+", b"data")
    assert m.size() == 4
    assert m.tell() == 0
    m.zero(5)
    m.seek(3)
    assert m.size() == 5
    assert m.tell() == 3


def test_extents():
    m = memory.Backend("r+", b"data")
    assert list(m.extents()) == [image.ZeroExtent(0, 4, False)]
