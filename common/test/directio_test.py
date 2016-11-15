# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import cStringIO
import pytest
import string

from collections import deque

from ovirt_imageio_common import directio
from ovirt_imageio_common import errors


def fill(s, size):
    count, rest = divmod(size, len(s))
    return s * count + s[:rest]


BUFFER = fill(string.uppercase, directio.BUFFERSIZE)
PARTIAL = fill(string.lowercase, directio.BLOCKSIZE)
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
    dst = cStringIO.StringIO()
    op = directio.Send(str(src), dst, size, offset=offset)
    op.run()
    return dst.getvalue()


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
    src = cStringIO.StringIO(data)
    op = directio.Receive(str(dst), src, size, offset=offset)
    op.run()
    with open(str(dst), "rb") as f:
        f.seek(offset)
        return f.read()


def test_send_repr():
    op = directio.Send("/path", None, 200, offset=24)
    rep = repr(op)
    assert "Send" in rep
    assert "path='/path' size=200 offset=24 buffersize=512 done=0" in rep


def test_recv_repr():
    op = directio.Receive("/path", None, 100, offset=42)
    rep = repr(op)
    assert "Receive" in rep
    assert "path='/path' size=100 offset=42 buffersize=512 done=0" in rep


@pytest.mark.parametrize("size,rounded", [
    (0, 0),
    (1, 512),
    (512, 512),
    (513, 1024),
])
def test_round_up(size, rounded):
    assert directio.round_up(size) == rounded


class UnbufferedStream(object):
    """
    Unlike regular file object, read may return any amount of bytes up to the
    requested size. This behavior is probably the result of doing one syscall
    per read, without any buffering.

    This simulate libvirt stream behavior used to copy imaged directly from
    libvirt.
    https://libvirt.org/html/libvirt-libvirt-stream.html#virStreamRecv
    """

    def __init__(self, chunks):
        self.chunks = deque(chunks)

    def read(self, size):
        if not self.chunks:
            return ''
        chunk = self.chunks.popleft()
        res = chunk[:size]
        chunk = chunk[size:]
        if chunk:
            self.chunks.appendleft(chunk)
        return res


def test_unbuffered_stream_more():
    chunks = ["1" * 256,
              "2" * 256,
              "3" * 42,
              "4" * 256]
    s = UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(512)
    assert b == chunks[0]
    # Chunk 2
    b = s.read(512)
    assert b == chunks[1]
    # Chunk 3
    b = s.read(512)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(512)
    assert b == chunks[3]
    # Empty
    b = s.read(512)
    assert b == ''
    b = s.read(512)
    assert b == ''


def test_unbuffered_stream_less():
    chunks = ["1" * 256,
              "2" * 256,
              "3" * 42,
              "4" * 256]
    s = UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(128)
    assert b == chunks[0][:128]
    b = s.read(128)
    assert b == chunks[0][128:]
    # Chunk 2
    b = s.read(128)
    assert b == chunks[1][:128]
    b = s.read(128)
    assert b == chunks[1][128:]
    # Chunk 3
    b = s.read(128)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(128)
    assert b == chunks[3][:128]
    b = s.read(128)
    assert b == chunks[3][128:]
    # Empty
    b = s.read(128)
    assert b == ''
    b = s.read(128)
    assert b == ''


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
    src = UnbufferedStream(chunks)
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
    src = cStringIO.StringIO(data)
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
    dst = cStringIO.StringIO()
    op = directio.Send(str(src), dst, offset=offset)
    op.run()
    assert dst.getvalue() == data[offset:]
