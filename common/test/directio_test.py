# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
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

from ovirt_imageio_common import directio
from ovirt_imageio_common import ops
from ovirt_imageio_common import errors
from ovirt_imageio_common.backends import file

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
    dst = tmpdir.join("dst")
    dst.write("x" * offset)
    src = io.BytesIO(data)
    op = directio.Receive(str(dst), src, size, offset=offset)
    op.run()
    with open(str(dst), "rb") as f:
        f.seek(offset)
        return f.read()


def test_receive_busy(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, file.BLOCKSIZE)
    assert op.active


def test_receive_close_on_success(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, file.BLOCKSIZE)
    op.run()
    assert not op.active


def test_receive_close_on_error(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, file.BLOCKSIZE + 1)
    with pytest.raises(errors.PartialContent):
        op.run()
    assert not op.active


def test_receive_close_twice(tmpfile):
    src = io.BytesIO(b"x" * file.BLOCKSIZE)
    op = directio.Receive(str(tmpfile), src, file.BLOCKSIZE)
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
    dst = tmpdir.join("src")
    data = b"x" * ops.BUFFERSIZE * 2
    dst.write(data)
    size = len(data)
    src = io.BytesIO(b"X" * size)
    op = directio.Receive(str(dst), src, size, **extra)
    op.run()
    with io.open(str(dst), "rb") as f:
        assert f.read() == src.getvalue()
    assert fsync_calls[0] == calls


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
    src = testutil.UnbufferedStream(chunks)
    op = directio.Receive(str(dst), src, size, buffersize=bufsize)
    op.run()
    return dst.read()


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    testutil.BUFFER * 2,
    testutil.BUFFER + testutil.BLOCK * 2,
    testutil.BUFFER + testutil.BLOCK + testutil.BYTES,
    testutil.BLOCK * 2,
    testutil.BLOCK + testutil.BYTES,
], ids=testutil.head)
def test_receive_no_size(tmpdir, data, offset):
    dst = tmpdir.join("dst")
    dst.write("x" * offset)
    src = io.BytesIO(data)
    op = directio.Receive(str(dst), src, offset=offset)
    op.run()
    assert dst.read()[offset:] == data
