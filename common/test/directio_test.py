# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import io
import os
import string
import sys

from contextlib import closing

import pytest

from ovirt_imageio_common import directio
from ovirt_imageio_common import errors

from . import ioutil

pytestmark = pytest.mark.skipif(sys.version_info[0] > 2,
                                reason='needs porting to python 3')


def fill(s, size):
    count, rest = divmod(size, len(s))
    return s * count + s[:rest]


BUFFER = fill(string.ascii_uppercase, directio.BUFFERSIZE)
PARTIAL = fill(string.ascii_lowercase, directio.BLOCKSIZE)
BYTES = fill(string.digits, 42)


def head(str):
    return str[:10]


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + PARTIAL * 2,
    BUFFER + PARTIAL + BYTES,
    PARTIAL * 2,
    PARTIAL + BYTES,
], ids=head)
def test_send(tmpdir, data, offset):
    size = len(data) - offset
    expected = data[offset:]
    assert send(tmpdir, data, size, offset=offset) == expected


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize(
    "size", [511, 513, len(BUFFER) + 511, len(BUFFER) + 513])
def test_send_partial(tmpdir, size, offset):
    data = BUFFER * 2
    expected = data[offset:offset + size]
    assert send(tmpdir, data, size, offset=offset) == expected


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + PARTIAL * 2,
    BUFFER + PARTIAL + BYTES,
    PARTIAL * 2,
    PARTIAL + BYTES,
], ids=head)
def test_send_partial_content(tmpdir, data, offset):
    size = len(data) - offset
    with pytest.raises(errors.PartialContent) as e:
        send(tmpdir, data[:-1], size, offset=offset)
    assert e.value.requested == size
    assert e.value.available == size - 1


def send(tmpdir, data, size, offset=0):
    src = tmpdir.join("src")
    src.write(data)
    dst = io.BytesIO()
    op = directio.Send(str(src), dst, size, offset=offset)
    op.run()
    return dst.getvalue()


@pytest.fixture
def tmpfile(tmpdir):
    f = tmpdir.join("tmpfile")
    f.write(b"x" * directio.BLOCKSIZE)
    return f


def test_send_busy(tmpfile):
    op = directio.Send(str(tmpfile), io.BytesIO(), tmpfile.size())
    next(iter(op))
    assert op.active


def test_send_close_on_success(tmpfile):
    op = directio.Send(str(tmpfile), io.BytesIO(), tmpfile.size())
    op.run()
    assert not op.active


def test_send_close_on_error(tmpfile):
    op = directio.Send(str(tmpfile), io.BytesIO(), tmpfile.size() + 1)
    with pytest.raises(errors.PartialContent):
        op.run()
    assert not op.active


def test_send_close_twice(tmpfile):
    op = directio.Send(str(tmpfile), io.BytesIO(), tmpfile.size())
    op.run()
    op.close()  # Should do nothing
    assert not op.active


def test_send_iterate_and_close(tmpfile):
    # Used when passing operation as app_iter on GET request.
    dst = io.BytesIO()
    op = directio.Send(str(tmpfile), dst, tmpfile.size())
    for chunk in op:
        dst.write(chunk)
    op.close()
    assert not op.active


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + PARTIAL * 2,
    BUFFER + PARTIAL + BYTES,
    PARTIAL * 2,
    PARTIAL + BYTES,
    BYTES,
], ids=head)
def test_receive(tmpdir, data, offset):
    assert receive(tmpdir, data, len(data), offset=offset) == data


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize(
    "size", [511, 513, len(BUFFER) + 511, len(BUFFER) + 513])
def test_receive_partial(tmpdir, size, offset):
    data = BUFFER * 2
    assert receive(tmpdir, data, size, offset=offset) == data[:size]


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + PARTIAL * 2,
    BUFFER + PARTIAL + BYTES,
    PARTIAL * 2,
    PARTIAL + BYTES,
    BYTES,
], ids=head)
def test_receive_partial_content(tmpdir, data, offset):
    with pytest.raises(errors.PartialContent) as e:
        receive(tmpdir, data[:-1], len(data), offset=offset)
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def receive(tmpdir, data, size, offset=0):
    dst = tmpdir.join("dst")
    dst.write("x" * offset)
    src = io.BytesIO(data)
    op = directio.Receive(str(dst), src, size, offset=offset)
    op.run()
    with open(str(dst), "rb") as f:
        f.seek(offset)
        return f.read()


def test_receive_busy(tmpfile):
    src = io.BytesIO(b"x" * directio.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, directio.BLOCKSIZE)
    assert op.active


def test_receive_close_on_success(tmpfile):
    src = io.BytesIO(b"x" * directio.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, directio.BLOCKSIZE)
    op.run()
    assert not op.active


def test_receive_close_on_error(tmpfile):
    src = io.BytesIO(b"x" * directio.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, directio.BLOCKSIZE + 1)
    with pytest.raises(errors.PartialContent):
        op.run()
    assert not op.active


def test_receive_close_twice(tmpfile):
    src = io.BytesIO(b"x" * directio.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, directio.BLOCKSIZE)
    op.run()
    op.close()  # should do nothing
    assert not op.active


def test_send_repr():
    op = directio.Send("/path", None, 200, offset=24)
    rep = repr(op)
    assert "Send" in rep
    assert "path='/path' size=200 offset=24 buffersize=512 done=0" in rep
    assert "active" in rep


def test_send_repr_active():
    op = directio.Send("/path", None)
    op.close()
    assert "active" not in repr(op)


def test_recv_repr():
    op = directio.Receive("/path", None, 100, offset=42)
    rep = repr(op)
    assert "Receive" in rep
    assert "path='/path' size=100 offset=42 buffersize=512 done=0" in rep
    assert "active" in rep


def test_recv_repr_active():
    op = directio.Receive("/path", None)
    op.close()
    assert "active" not in repr(op)


@pytest.mark.parametrize("size,rounded", [
    (0, 0),
    (1, 512),
    (512, 512),
    (513, 1024),
])
def test_round_up(size, rounded):
    assert directio.round_up(size) == rounded


@pytest.mark.parametrize("size,rounded", [
    (0, 0),
    (1, 0),
    (512, 512),
    (513, 512),
])
def test_round_down(size, rounded):
    assert directio.round_down(size) == rounded


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
    dst = tmpdir.join("dst")
    dst.write("")
    src = ioutil.UnbufferedStream(chunks)
    op = directio.Receive(str(dst), src, size, buffersize=bufsize)
    op.run()
    return dst.read()


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + PARTIAL * 2,
    BUFFER + PARTIAL + BYTES,
    PARTIAL * 2,
    PARTIAL + BYTES,
], ids=head)
def test_receive_no_size(tmpdir, data, offset):
    dst = tmpdir.join("dst")
    dst.write("x" * offset)
    src = io.BytesIO(data)
    op = directio.Receive(str(dst), src, offset=offset)
    op.run()
    assert dst.read()[offset:] == data


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + PARTIAL * 2,
    BUFFER + PARTIAL + BYTES,
    PARTIAL * 2,
    PARTIAL + BYTES,
], ids=head)
def test_send_no_size(tmpdir, data, offset):
    src = tmpdir.join("src")
    src.write(data)
    dst = io.BytesIO()
    op = directio.Send(str(src), dst, offset=offset)
    op.run()
    assert dst.getvalue() == data[offset:]


@pytest.mark.parametrize("offset,size", [
    # Aligned offset and size
    (0, directio.BLOCKSIZE),
    (0, directio.BUFFERSIZE - directio.BLOCKSIZE),
    (0, directio.BUFFERSIZE),
    (0, directio.BUFFERSIZE + directio.BLOCKSIZE),
    (0, directio.BUFFERSIZE * 2),
    (directio.BLOCKSIZE, directio.BLOCKSIZE),
    (directio.BUFFERSIZE, directio.BLOCKSIZE),
    (directio.BUFFERSIZE * 2 - directio.BLOCKSIZE, directio.BLOCKSIZE),
    # Unalinged size
    (0, 42),
    (0, directio.BLOCKSIZE + 42),
    (0, directio.BUFFERSIZE + 42),
    # Unaligned offset
    (42, directio.BLOCKSIZE),
    (directio.BLOCKSIZE + 42, directio.BLOCKSIZE),
    (directio.BUFFERSIZE + 42, directio.BLOCKSIZE),
    # Unaligned size and offset
    (42, directio.BLOCKSIZE - 42),
    (directio.BLOCKSIZE + 42, directio.BLOCKSIZE - 42),
    (directio.BUFFERSIZE + 42, directio.BUFFERSIZE - 42),
    (directio.BUFFERSIZE * 2 - 42, 42),
])
def test_zero(tmpdir, offset, size):
    dst = tmpdir.join("src")
    data = "x" * directio.BUFFERSIZE * 2
    dst.write(data)
    op = directio.Zero(str(dst), size, offset=offset)
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
    data = "x" * directio.BUFFERSIZE * 2
    dst.write(data)
    size = len(data)
    op = directio.Zero(str(dst), size, flush=flush)
    op.run()
    with io.open(str(dst), "rb") as f:
        assert f.read() == b"\0" * size
    assert fsync_calls[0] == calls


def test_open_write_only(tmpdir):
    path = str(tmpdir.join("path"))
    with directio.open(path, "w") as f, \
            closing(directio.aligned_buffer(512)) as buf:
        buf.write(b"x" * 512)
        f.write(buf)
    with io.open(path, "rb") as f:
        assert f.read() == b"x" * 512


def test_open_write_only_truncate(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 512)
    with directio.open(path, "w") as f:
        pass
    with io.open(path, "rb") as f:
        assert f.read() == b""


def test_open_read_only(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 512)
    with directio.open(path, "r") as f, \
            closing(directio.aligned_buffer(512)) as buf:
        f.readinto(buf)
        assert buf[:] == b"x" * 512


def test_open_read_write(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"a" * 512)
    with directio.open(path, "r+") as f, \
            closing(directio.aligned_buffer(512)) as buf:
        f.readinto(buf)
        buf[:] = b"b" * 512
        f.seek(0)
        f.write(buf)
    with io.open(path, "rb") as f:
        assert f.read() == b"b" * 512


@pytest.mark.parametrize("mode", ["r", "r+"])
def test_open_no_create(tmpdir, mode):
    path = str(tmpdir.join("path"))
    with pytest.raises(OSError) as e:
        with directio.open(path, mode):
            pass
    assert e.value.errno == errno.ENOENT


def test_open_no_direct_read_only(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 512)
    with directio.open(path, "r", direct=False) as f:
        assert f.read() == b"x" * 512


def test_open_no_direct_read_write(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"a" * 512)
    with directio.open(path, "r+", direct=False) as f:
        f.write(b"b" * 512)
        f.seek(0)
        assert f.read() == b"b" * 512


def test_open_no_direct_write_only(tmpdir):
    path = str(tmpdir.join("path"))
    with directio.open(path, "w", direct=False) as f:
        f.write(b"x" * 512)
    with io.open(path, "rb") as f:
        assert f.read() == b"x" * 512
