# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import sys

import pytest

from ovirt_imageio_common import ops
from ovirt_imageio_common import errors
from ovirt_imageio_common.backends import file
from ovirt_imageio_common.backends import memory

from . import testutil

# TODO: use backend block_size.
BLOCKSIZE = 512

pytestmark = pytest.mark.skipif(sys.version_info[0] > 2,
                                reason='needs porting to python 3')


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
], ids=testutil.head)
def test_send_full(data, offset):
    size = len(data) - offset
    expected = data[offset:]
    assert send(data, size, offset=offset) == expected


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("size", [
    511,
    513,
    len(testutil.BUFFER) + 511,
    len(testutil.BUFFER) + 513,
])
def test_send_partial(size, offset):
    data = testutil.BUFFER * 2
    expected = data[offset:offset + size]
    assert send(data, size, offset=offset) == expected


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
], ids=testutil.head)
def test_send_partial_content(data, offset):
    size = len(data) - offset
    with pytest.raises(errors.PartialContent) as e:
        send(data[:-1], size, offset=offset)
    assert e.value.requested == size
    assert e.value.available == size - 1


def send(data, size, offset=0):
    src = memory.Backend("r", data)
    dst = io.BytesIO()
    op = ops.Send(src, dst, size, offset=offset)
    op.run()
    return dst.getvalue()


def test_send_seek():
    src = memory.Backend("r", b"0123456789")
    src.seek(8)
    dst = io.BytesIO()
    op = ops.Send(src, dst, 5)
    op.run()
    assert dst.getvalue() == b"01234"


def test_send_busy():
    src = memory.Backend("r", b"data")
    op = ops.Send(src, io.BytesIO(), 4)
    assert op.active


def test_send_close_on_success():
    src = memory.Backend("r", b"data")
    op = ops.Send(src, io.BytesIO(), 4)
    op.run()
    assert not op.active


def test_send_close_on_error():
    src = memory.Backend("r", b"data")
    op = ops.Send(src, io.BytesIO(), 5)
    with pytest.raises(errors.PartialContent):
        op.run()
    assert not op.active


def test_send_close_twice():
    src = memory.Backend("r", b"data")
    op = ops.Send(src, io.BytesIO(), 4)
    op.run()
    op.close()  # Should do nothing
    assert not op.active


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
], ids=testutil.head)
def test_send_no_size(data, offset):
    src = memory.Backend("r", data)
    dst = io.BytesIO()
    op = ops.Send(src, dst, offset=offset)
    op.run()
    assert dst.getvalue() == data[offset:]


def test_send_repr():
    op = ops.Send(None, None, 200, offset=24)
    rep = repr(op)
    assert "Send" in rep
    assert "size=200 offset=24 buffersize=4096 done=0" in rep
    assert "active" in rep


def test_send_repr_active():
    op = ops.Send(None, None)
    op.close()
    assert "active" not in repr(op)


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
    testutil.BYTES,
], ids=testutil.head)
def test_receive(tmpurl, data, offset):
    assert receive(tmpurl, data, len(data), offset=offset) == data


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("size", [
    511,
    513,
    len(testutil.BUFFER) + 511,
    len(testutil.BUFFER) + 513,
])
def test_receive_partial(tmpurl, size, offset):
    data = testutil.BUFFER * 2
    assert receive(tmpurl, data, size, offset=offset) == data[:size]


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
    testutil.BYTES,
], ids=testutil.head)
def test_receive_partial_content(tmpurl, data, offset):
    with pytest.raises(errors.PartialContent) as e:
        receive(tmpurl, data[:-1], len(data), offset=offset)
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def receive(tmpurl, data, size, offset=0):
    with io.open(tmpurl.path, "wb") as f:
        f.write("x" * offset)
    with file.open(tmpurl, "r+") as dst:
        src = io.BytesIO(data)
        op = ops.Receive(dst, src, size, offset=offset)
        op.run()
    with io.open(tmpurl.path, "rb") as f:
        f.seek(offset)
        return f.read(size)


def test_receive_padd_to_block_size(tmpurl):
    with io.open(tmpurl.path, "wb") as f:
        f.write("x" * 400)
    size = 200
    offset = 300
    padding = BLOCKSIZE - size - offset
    src = io.BytesIO(b"y" * size)
    with file.open(tmpurl, "r+") as dst:
        op = ops.Receive(dst, src, size, offset=offset)
        op.run()
    with io.open(tmpurl.path, "rb") as f:
        # Data before offset is not modified.
        assert f.read(300) == b"x" * offset
        # Data after offset is modifed, flie extended.
        assert f.read(200) == b"y" * size
        # File padded to block size with zeroes.
        assert f.read() == b"\0" * padding


def test_receive_seek():
    dst = memory.Backend("r+", b"a" * 10)
    dst.seek(8)
    src = io.BytesIO(b"b" * 5)
    op = ops.Receive(dst, src, 5)
    op.run()
    dst.seek(0)
    b = bytearray(11)
    n = dst.readinto(b)
    assert n == 10
    assert b == b"bbbbbaaaaa\0"


def test_receive_busy(tmpurl):
    src = io.BytesIO(b"x" * BLOCKSIZE)
    with file.open(tmpurl, "r+") as dst:
        op = ops.Receive(dst, src, BLOCKSIZE)
        assert op.active


def test_receive_close_on_success(tmpurl):
    src = io.BytesIO(b"x" * BLOCKSIZE)
    with file.open(tmpurl, "r+") as dst:
        op = ops.Receive(dst, src, BLOCKSIZE)
        op.run()
        assert not op.active


def test_receive_close_on_error(tmpurl):
    src = io.BytesIO(b"x" * BLOCKSIZE)
    with file.open(tmpurl, "r+") as dst:
        op = ops.Receive(dst, src, BLOCKSIZE + 1)
        with pytest.raises(errors.PartialContent):
            op.run()
        assert not op.active


def test_receive_close_twice(tmpurl):
    src = io.BytesIO(b"x" * BLOCKSIZE)
    with file.open(tmpurl, "r+") as dst:
        op = ops.Receive(dst, src, BLOCKSIZE)
        op.run()
        op.close()  # should do nothing
        assert not op.active


@pytest.mark.parametrize("extra, dirty", [
    ({}, False),  # Flushes by default for backward compatibility.
    ({"flush": True}, False),
    ({"flush": False}, True),
])
def test_receive_flush(extra, dirty):
    size = 4096
    dst = memory.Backend("r+", b"a" * size)
    src = io.BytesIO(b"b" * size)
    op = ops.Receive(dst, src, size, **extra)
    op.run()
    assert dst.dirty == dirty


def test_recv_repr():
    op = ops.Receive(None, None, 100, offset=42)
    rep = repr(op)
    assert "Receive" in rep
    assert "size=100 offset=42 buffersize=4096 done=0" in rep
    assert "active" in rep


def test_recv_repr_active():
    op = ops.Receive(memory.Backend("r+"), None)
    op.close()
    assert "active" not in repr(op)


@pytest.mark.parametrize("bufsize", [512, 1024, 2048])
def test_receive_unbuffered_stream(tmpurl, bufsize):
    chunks = ["1" * 1024,
              "2" * 1024,
              "3" * 42,
              "4" * 982]
    data = ''.join(chunks)
    assert receive_unbuffered(tmpurl, chunks, len(data), bufsize) == data


def test_receive_unbuffered_stream_partial_content(tmpurl):
    chunks = ["1" * 1024,
              "2" * 1024,
              "3" * 42,
              "4" * 982]
    data = ''.join(chunks)
    with pytest.raises(errors.PartialContent):
        receive_unbuffered(tmpurl, chunks, len(data) + 1, 2048)


def receive_unbuffered(tmpurl, chunks, size, bufsize):
    src = testutil.UnbufferedStream(chunks)
    with file.open(tmpurl, "r+") as dst:
        op = ops.Receive(dst, src, size, buffersize=bufsize)
        op.run()
        with io.open(tmpurl.path, "rb") as f:
            return f.read()


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
], ids=testutil.head)
def test_receive_no_size(tmpurl, data, offset):
    with io.open(tmpurl.path, "wb") as f:
        f.write("x" * offset)
    src = io.BytesIO(data)
    with file.open(tmpurl, "r+") as dst:
        op = ops.Receive(dst, src, offset=offset)
        op.run()
    with io.open(tmpurl.path, "rb") as f:
        f.seek(offset)
        assert f.read(len(data)) == data


@pytest.mark.parametrize("sparse", [True, False])
@pytest.mark.parametrize("offset,size", [
    # Aligned offset and size
    (0, BLOCKSIZE),
    (0, ops.BUFFERSIZE - BLOCKSIZE),
    (0, ops.BUFFERSIZE),
    (0, ops.BUFFERSIZE + BLOCKSIZE),
    (0, ops.BUFFERSIZE * 2),
    (BLOCKSIZE, BLOCKSIZE),
    (ops.BUFFERSIZE, BLOCKSIZE),
    (ops.BUFFERSIZE * 2 - BLOCKSIZE, BLOCKSIZE),
    # Unalinged size
    (0, 42),
    (0, BLOCKSIZE + 42),
    (0, ops.BUFFERSIZE + 42),
    # Unaligned offset
    (42, BLOCKSIZE),
    (BLOCKSIZE + 42, BLOCKSIZE),
    (ops.BUFFERSIZE + 42, BLOCKSIZE),
    # Unaligned size and offset
    (42, BLOCKSIZE - 42),
    (BLOCKSIZE + 42, BLOCKSIZE - 42),
    (ops.BUFFERSIZE + 42, ops.BUFFERSIZE - 42),
    (ops.BUFFERSIZE * 2 - 42, 42),
])
def test_zero(tmpurl, offset, size, sparse):
    data = "x" * ops.BUFFERSIZE * 2
    with io.open(tmpurl.path, "wb") as f:
        f.write(data)
    with file.open(tmpurl, "r+", sparse=sparse) as dst:
        op = ops.Zero(dst, size, offset=offset)
        op.run()
    with io.open(tmpurl.path, "rb") as f:
        assert f.read(offset) == data[:offset]
        assert f.read(size) == b"\0" * size
        assert f.read() == data[offset + size:]


def test_zero_seek():
    dst = memory.Backend("r+", b"a" * 10)
    dst.seek(8)
    op = ops.Zero(dst, 5)
    op.run()
    dst.seek(0)
    b = bytearray(11)
    n = dst.readinto(b)
    assert n == 10
    assert b == b"\0\0\0\0\0aaaaa\0"


@pytest.mark.parametrize("extra, dirty", [
    ({}, True),  # Does not flush by default.
    ({"flush": True}, False),
    ({"flush": False}, True),
])
def test_zero_flush(extra, dirty):
    size = 4096
    dst = memory.Backend("r+", b"a" * size)
    op = ops.Zero(dst, size, **extra)
    op.run()
    assert dst.dirty == dirty


def test_zero_busy():
    op = ops.Zero(memory.Backend("r+"), 4096)
    assert op.active


def test_zero_close_on_success():
    op = ops.Zero(memory.Backend("r+"), 4096)
    op.run()
    assert not op.active


def test_zero_close_on_error():
    # Use readonly backend to trigger IOError on zero().
    dst = memory.Backend("r")
    op = ops.Zero(dst, 4096)
    with pytest.raises(IOError):
        op.run()
    assert not op.active


def test_zero_close_twice():
    op = ops.Zero(memory.Backend("r+"), 4096)
    op.run()
    op.close()  # should do nothing
    assert not op.active


def test_zero_repr():
    op = ops.Zero(memory.Backend("r+"), 4096)
    rep = repr(op)
    assert "Zero" in rep
    assert "offset=0" in rep
    assert "size=4096" in rep
    assert "done=0" in rep
    assert "active" in rep


def test_zero_repr_active():
    op = ops.Zero(memory.Backend("r+"), 4096)
    op.close()
    assert "active" not in repr(op)


def test_flush():
    dst = memory.Backend("r+")
    dst.write(b"x")
    op = ops.Flush(dst)
    op.run()
    assert not dst.dirty


def test_flush_busy():
    op = ops.Flush(memory.Backend("r+"))
    assert op.active


def test_flush_close_on_success():
    op = ops.Flush(memory.Backend("r+"))
    op.run()
    assert not op.active


def test_flush_close_on_error():

    def flush():
        raise OSError

    dst = memory.Backend("r")
    dst.flush = flush
    op = ops.Flush(dst)
    with pytest.raises(OSError):
        op.run()
    assert not op.active


def test_flush_close_twice():
    op = ops.Flush(memory.Backend("r+"))
    op.run()
    op.close()  # should do nothing
    assert not op.active


def test_flush_repr():
    op = ops.Flush(memory.Backend("r"))
    rep = repr(op)
    assert "Flush" in rep
    assert "done=0" in rep
    assert "active" in rep


def test_flush_repr_active():
    op = ops.Flush(memory.Backend("r"))
    op.close()
    assert "active" not in repr(op)
