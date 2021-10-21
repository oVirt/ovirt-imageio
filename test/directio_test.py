# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import io
import os

import pytest

from ovirt_imageio._internal import directio
from ovirt_imageio._internal import errors
from ovirt_imageio._internal import util

from . import testutil

# Legacy code supports only 512 bytes.
BLOCKSIZE = 512


def fill(b, size):
    count, rest = divmod(size, len(b))
    return b * count + b[:rest]


BUFFER = fill(b"ABCDEFGHIJ", 1024**2)
BLOCK = fill(b"abcdefghij", 512)
BYTES = fill(b"0123456789", 42)


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + BLOCK * 2,
    BUFFER + BLOCK + BYTES,
    BLOCK * 2,
    BLOCK + BYTES,
    BYTES,
], ids=testutil.head)
def test_receive(tmpfile, data, offset):
    assert receive(tmpfile, data, len(data), offset=offset) == data


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("size", [
    511,
    513,
    len(BUFFER) + 511,
    len(BUFFER) + 513,
])
def test_receive_partial(tmpfile, size, offset):
    data = BUFFER * 2
    assert receive(tmpfile, data, size, offset=offset) == data[:size]


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + BLOCK * 2,
    BUFFER + BLOCK + BYTES,
    BLOCK * 2,
    BLOCK + BYTES,
    BYTES,
], ids=testutil.head)
def test_receive_partial_content(tmpfile, data, offset):
    with pytest.raises(errors.PartialContent) as e:
        receive(tmpfile, data[:-1], len(data), offset=offset)
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def receive(tmpfile, data, size, offset=0):
    with open(tmpfile, "wb") as f:
        f.write(b"x" * offset)
    src = io.BytesIO(data)
    op = directio.Receive(tmpfile, src, size, offset=offset)
    op.run()
    with open(tmpfile, "rb") as f:
        f.seek(offset)
        return f.read(size)


@pytest.mark.parametrize("extra, calls", [
    ({}, 1),  # Flushes by default.
    ({"flush": True}, 1),
    ({"flush": False}, 0),
])
def test_receive_flush(tmpfile, monkeypatch, extra, calls):
    # This would be much cleaner when we add backend object implementing flush.
    fsync = os.fsync
    fsync_calls = [0]

    def counted_fsync(fd):
        fsync_calls[0] += 1
        fsync(fd)

    monkeypatch.setattr("os.fsync", counted_fsync)
    data = b"x" * 1024**2 * 2
    with open(tmpfile, "wb") as f:
        f.write(data)
    size = len(data)
    src = io.BytesIO(b"X" * size)
    op = directio.Receive(tmpfile, src, size, **extra)
    op.run()
    with io.open(tmpfile, "rb") as f:
        assert f.read() == src.getvalue()
    assert fsync_calls[0] == calls


def test_recv_repr(tmpfile):
    op = directio.Receive(tmpfile, None, 100, offset=42)
    rep = repr(op)
    assert "Receive" in rep
    assert "size=100 offset=42 done=0" in rep


@pytest.mark.parametrize("bufsize", [512, 1024, 2048])
def test_receive_unbuffered_stream(tmpfile, bufsize):
    chunks = [b"a" * 8192,
              b"b" * 42,
              b"c" * (8192 - 42)]
    data = b''.join(chunks)
    assert receive_unbuffered(tmpfile, chunks, len(data), bufsize) == data


def test_receive_unbuffered_stream_partial_content(tmpfile):
    chunks = [b"a" * 8192,
              b"b" * 42,
              b"c" * (8192 - 42)]
    data = b''.join(chunks)
    with pytest.raises(errors.PartialContent):
        receive_unbuffered(tmpfile, chunks, len(data) + 1, 2048)


def receive_unbuffered(tmpfile, chunks, size, bufsize):
    src = util.UnbufferedStream(chunks)
    op = directio.Receive(tmpfile, src, size, buffersize=bufsize)
    op.run()
    with open(tmpfile, "rb") as f:
        return f.read()


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    BUFFER * 2,
    BUFFER + BLOCK * 2,
    BUFFER + BLOCK + BYTES,
    BLOCK * 2,
    BLOCK + BYTES,
], ids=testutil.head)
def test_receive_no_size(tmpfile, data, offset):
    with open(tmpfile, "wb") as f:
        f.write(b"x" * offset)
    src = io.BytesIO(data)
    op = directio.Receive(tmpfile, src, offset=offset)
    op.run()
    with io.open(tmpfile, "rb") as f:
        f.seek(offset)
        assert f.read(len(data)) == data


def test_receive_padd_to_block_size(tmpfile):
    with open(tmpfile, "wb") as f:
        f.write(b"x" * 400)
    size = 200
    offset = 300
    padding = BLOCKSIZE - size - offset
    src = io.BytesIO(b"y" * size)
    op = directio.Receive(tmpfile, src, size, offset=offset)
    op.run()
    with open(tmpfile, "rb") as f:
        # Data before offset is not modified.
        assert f.read(300) == b"x" * offset
        # Data after offset is modifed, flie extended.
        assert f.read(200) == b"y" * size
        # File padded to block size with zeroes.
        assert f.read() == b"\0" * padding
