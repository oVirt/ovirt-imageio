# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import os
import sys

import pytest

from ovirt_imageio_common import ops
from ovirt_imageio_common import errors
from ovirt_imageio_common.backends import file
from ovirt_imageio_common.backends import memory

from . import testutil

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
    next(iter(op))
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


def test_send_iterate_and_close():
    # Used when passing operation as app_iter on GET request.
    src = memory.Backend("r", testutil.BUFFER)
    dst = io.BytesIO()
    op = ops.Send(src, dst, len(testutil.BUFFER))
    for chunk in op:
        dst.write(chunk)
    op.close()
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
    assert "size=200 offset=24 buffersize=512 done=0" in rep
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
def test_receive(tmpdir, data, offset):
    assert receive(tmpdir, data, len(data), offset=offset) == data


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("size", [
    511,
    513,
    len(testutil.BUFFER) + 511,
    len(testutil.BUFFER) + 513,
])
def test_receive_partial(tmpdir, size, offset):
    data = testutil.BUFFER * 2
    assert receive(tmpdir, data, size, offset=offset) == data[:size]


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
    testutil.BYTES,
], ids=testutil.head)
def test_receive_partial_content(tmpdir, data, offset):
    with pytest.raises(errors.PartialContent) as e:
        receive(tmpdir, data[:-1], len(data), offset=offset)
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def receive(tmpdir, data, size, offset=0):
    dst_file = tmpdir.join("dst")
    dst_file.write("x" * offset)
    with file.open(str(dst_file), "r+") as dst:
        src = io.BytesIO(data)
        op = ops.Receive(dst, src, size, offset=offset)
        op.run()
    with open(str(dst_file), "rb") as f:
        f.seek(offset)
        return f.read(size)


def test_receive_padd_to_block_size(tmpdir):
    dst_file = tmpdir.join("dst")
    dst_file.write("x" * 400)
    size = 200
    offset = 300
    padding = file.BLOCKSIZE - size - offset
    src = io.BytesIO(b"y" * size)
    with file.open(str(dst_file), "r+") as dst:
        op = ops.Receive(dst, src, size, offset=offset)
        op.run()
    with open(str(dst_file), "rb") as f:
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


def test_receive_busy(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    with file.open(tmpfile, "r+") as dst:
        op = ops.Receive(dst, src, file.BLOCKSIZE)
        assert op.active


def test_receive_close_on_success(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    with file.open(tmpfile, "r+") as dst:
        op = ops.Receive(dst, src, file.BLOCKSIZE)
        op.run()
        assert not op.active


def test_receive_close_on_error(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    with file.open(tmpfile, "r+") as dst:
        op = ops.Receive(dst, src, file.BLOCKSIZE + 1)
        with pytest.raises(errors.PartialContent):
            op.run()
        assert not op.active


def test_receive_close_twice(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    with file.open(tmpfile, "r+") as dst:
        op = ops.Receive(dst, src, file.BLOCKSIZE)
        op.run()
        op.close()  # should do nothing
        assert not op.active


@pytest.mark.parametrize("extra, calls", [
    ({}, 1),  # Flushes by default.
    ({"flush": True}, 1),
    ({"flush": False}, 0),
])
def test_receive_flush(tmpdir, monkeypatch, extra, calls):
    # This would be much cleaner when we add backend object implementing flush.
    fsync = os.fsync
    fsync_calls = [0]

    def counted_fsync(fd):
        fsync_calls[0] += 1
        fsync(fd)

    monkeypatch.setattr("os.fsync", counted_fsync)
    dst_file = tmpdir.join("src")
    data = b"x" * ops.BUFFERSIZE * 2
    dst_file.write(data)
    size = len(data)
    src = io.BytesIO(b"X" * size)
    with file.open(str(dst_file), "r+") as dst:
        op = ops.Receive(dst, src, size, **extra)
        op.run()
        with io.open(str(dst_file), "rb") as f:
            assert f.read() == src.getvalue()
        assert fsync_calls[0] == calls


def test_recv_repr():
    op = ops.Receive(None, None, 100, offset=42)
    rep = repr(op)
    assert "Receive" in rep
    assert "size=100 offset=42 buffersize=512 done=0" in rep
    assert "active" in rep


def test_recv_repr_active():
    op = ops.Receive(memory.Backend("r+"), None)
    op.close()
    assert "active" not in repr(op)


@pytest.mark.parametrize("bufsize", [512, 1024, 2048])
def test_receive_unbuffered_stream(tmpdir, bufsize):
    chunks = ["1" * 1024,
              "2" * 1024,
              "3" * 42,
              "4" * 982]
    data = ''.join(chunks)
    assert receive_unbuffered(tmpdir, chunks, len(data), bufsize) == data


def test_receive_unbuffered_stream_partial_content(tmpdir):
    chunks = ["1" * 1024,
              "2" * 1024,
              "3" * 42,
              "4" * 982]
    data = ''.join(chunks)
    with pytest.raises(errors.PartialContent):
        receive_unbuffered(tmpdir, chunks, len(data) + 1, 2048)


def receive_unbuffered(tmpdir, chunks, size, bufsize):
    dst_file = tmpdir.join("dst")
    dst_file.write("")
    src = testutil.UnbufferedStream(chunks)
    with file.open(str(dst_file), "r+") as dst:
        op = ops.Receive(dst, src, size, buffersize=bufsize)
        op.run()
        with open(str(dst_file), "rb") as f:
            return f.read()


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
], ids=testutil.head)
def test_receive_no_size(tmpdir, data, offset):
    dst_file = tmpdir.join("dst")
    dst_file.write("x" * offset)
    src = io.BytesIO(data)
    with file.open(str(dst_file), "r+") as dst:
        op = ops.Receive(dst, src, offset=offset)
        op.run()
    with io.open(str(dst_file), "rb") as f:
        f.seek(offset)
        assert f.read(len(data)) == data


@pytest.mark.parametrize("sparse", [True, False])
@pytest.mark.parametrize("offset,size", [
    # Aligned offset and size
    (0, file.BLOCKSIZE),
    (0, ops.BUFFERSIZE - file.BLOCKSIZE),
    (0, ops.BUFFERSIZE),
    (0, ops.BUFFERSIZE + file.BLOCKSIZE),
    (0, ops.BUFFERSIZE * 2),
    (file.BLOCKSIZE, file.BLOCKSIZE),
    (ops.BUFFERSIZE, file.BLOCKSIZE),
    (ops.BUFFERSIZE * 2 - file.BLOCKSIZE, file.BLOCKSIZE),
    # Unalinged size
    (0, 42),
    (0, file.BLOCKSIZE + 42),
    (0, ops.BUFFERSIZE + 42),
    # Unaligned offset
    (42, file.BLOCKSIZE),
    (file.BLOCKSIZE + 42, file.BLOCKSIZE),
    (ops.BUFFERSIZE + 42, file.BLOCKSIZE),
    # Unaligned size and offset
    (42, file.BLOCKSIZE - 42),
    (file.BLOCKSIZE + 42, file.BLOCKSIZE - 42),
    (ops.BUFFERSIZE + 42, ops.BUFFERSIZE - 42),
    (ops.BUFFERSIZE * 2 - 42, 42),
])
def test_zero(tmpdir, offset, size, sparse):
    dst = tmpdir.join("src")
    data = "x" * ops.BUFFERSIZE * 2
    dst.write(data)
    op = ops.Zero(str(dst), size, offset=offset, sparse=sparse)
    op.run()
    with io.open(str(dst), "rb") as f:
        assert f.read(offset) == data[:offset]
        assert f.read(size) == b"\0" * size
        assert f.read() == data[offset + size:]


@pytest.mark.parametrize("flush, calls", [(True, 1), (False, 0)])
def test_zero_flush(tmpdir, monkeypatch, flush, calls):
    # This would be much cleaner when we add backend object implementing flush.
    fsync = os.fsync
    fsync_calls = [0]

    def counted_fsync(fd):
        fsync_calls[0] += 1
        fsync(fd)

    monkeypatch.setattr("os.fsync", counted_fsync)
    dst = tmpdir.join("src")
    data = "x" * ops.BUFFERSIZE * 2
    dst.write(data)
    size = len(data)
    op = ops.Zero(str(dst), size, flush=flush)
    op.run()
    with io.open(str(dst), "rb") as f:
        assert f.read() == b"\0" * size
    assert fsync_calls[0] == calls


def test_zero_busy():
    op = ops.Zero("/no/such/file", 100)
    assert op.active


def test_zero_close_on_success(tmpfile):
    op = ops.Zero(tmpfile, 100)
    op.run()
    assert not op.active


def test_zero_close_on_error():
    op = ops.Zero("/no/such/file", 100)
    with pytest.raises(OSError):
        op.run()
    assert not op.active


def test_zero_close_twice(tmpfile):
    op = ops.Zero(tmpfile, 100)
    op.run()
    op.close()  # should do nothing
    assert not op.active


def test_zero_repr():
    op = ops.Zero("/path", 100)
    rep = repr(op)
    assert "Zero" in rep
    assert "path='/path'" in rep
    assert "offset=0" in rep
    assert "size=100" in rep
    assert "done=0" in rep
    assert "active" in rep


def test_zero_repr_active():
    op = ops.Zero("/path", 100)
    op.close()
    assert "active" not in repr(op)


def test_flush(tmpdir, monkeypatch):
    # This would be much cleaner when we add backend object implementing flush.
    fsync = os.fsync
    fsync_calls = [0]

    def counted_fsync(fd):
        fsync_calls[0] += 1
        fsync(fd)

    monkeypatch.setattr("os.fsync", counted_fsync)
    dst = tmpdir.join("src")
    dst.write("x" * ops.BUFFERSIZE)
    op = ops.Flush(str(dst))
    op.run()
    assert fsync_calls[0] == 1


def test_flush_busy():
    op = ops.Flush("/no/such/file")
    assert op.active


def test_flush_close_on_success(tmpfile):
    op = ops.Flush(tmpfile)
    op.run()
    assert not op.active


def test_flush_close_on_error():
    op = ops.Flush("/no/such/file")
    with pytest.raises(OSError):
        op.run()
    assert not op.active


def test_flush_close_twice(tmpfile):
    op = ops.Flush(tmpfile)
    op.run()
    op.close()  # should do nothing
    assert not op.active


def test_flush_repr():
    op = ops.Flush("/path")
    rep = repr(op)
    assert "Flush" in rep
    assert "path='/path'" in rep
    assert "done=0" in rep
    assert "active" in rep


def test_flush_repr_active():
    op = ops.Flush("/path")
    op.close()
    assert "active" not in repr(op)
