# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import io
import os

import pytest
import userstorage

from ovirt_imageio._internal import errors
from ovirt_imageio._internal import ops
from ovirt_imageio._internal import stats
from ovirt_imageio._internal import util
from ovirt_imageio._internal.backends import file
from ovirt_imageio._internal.backends import memory

from . import storage

BACKENDS = userstorage.load_config("storage.py").BACKENDS


@pytest.fixture(
    params=[
        BACKENDS["file-512-xfs"],
        BACKENDS["file-4k-xfs"],
    ],
    ids=str
)
def user_file(request):
    with storage.Backend(request.param) as backend:
        yield backend


# Common offset and size parameters.
OFFSET_SIZE = [
    pytest.param(0, 8192, id="small-aligned"),
    pytest.param(0, 511, id="small-partial-block"),
    pytest.param(42, 512 - 42 - 1, id="small-partial-block-offset"),
    pytest.param(42, 8192 - 42, id="small-unaligned-offset"),
    pytest.param(42, 8192, id="small-unaligned-offset-and-size"),
    pytest.param(0, 1024**2 * 2, id="large-aligned"),
    pytest.param(42, 1024**2 * 2 - 42, id="large-unaligned-offset"),
    pytest.param(42, 1024**2 * 2, id="large-unaligned-offset-and-size"),
]


class Operation(ops.Operation):

    def _run(self):
        if self._canceled:
            raise ops.Canceled
        self._done = 100
        return self._done


def test_run():
    op = Operation(size=100)
    assert op.offset == 0
    assert op.size == 100
    assert op.done == 0
    assert op.run() == 100
    assert op.done == 100


def test_cancel():
    op = Operation(size=100)
    assert op.offset == 0
    assert op.size == 100
    assert op.done == 0
    op.cancel()
    with pytest.raises(ops.Canceled):
        op.run()
    assert op.offset == 0
    assert op.done == 0


def test_stats(fake_time):

    class Operation(ops.Operation):

        name = "foo"

        def _run(self):

            # Simulate reading from storage...
            with self._record("read") as s:
                fake_time.now += 0.01
                s.bytes += self._size

            # Simulate writing to client socket...
            with self._record("write") as s:
                fake_time.now += 0.005
                s.bytes += self._size

            self._done = self._size

    clock = stats.Clock(now=fake_time.monotonic_time)
    op = Operation(size=8 * 1024**2, clock=clock)
    op.run()
    times = str(clock)
    assert times == (
        "[foo 1 ops, 0.015000 s, 8.00 MiB, 533.33 MiB/s] "
        "[foo.read 1 ops, 0.010000 s, 8.00 MiB, 800.00 MiB/s] "
        "[foo.write 1 ops, 0.005000 s, 8.00 MiB, 1.56 GiB/s]"
    )


@pytest.mark.parametrize("trailer", [
    pytest.param(0, id="no-trailer"),
    pytest.param(8192, id="trailer"),
])
@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_read_full(user_file, offset, size, trailer):
    data = b"b" * size

    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * offset)
        f.write(data)
        f.write(b"c" * trailer)

    dst = io.BytesIO()
    with file.open(user_file.url, "r") as src, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Read(src, dst, buf, size, offset=offset)
        op.run()

    assert dst.getvalue() == data


@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_read_partial_content(user_file, offset, size):
    with io.open(user_file.path, "wb") as f:
        f.truncate(offset + size - 1)

    dst = io.BytesIO()
    with file.open(user_file.url, "r") as src, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Read(src, dst, buf, size, offset=offset)
        with pytest.raises(errors.PartialContent) as e:
            op.run()

    assert e.value.requested == size
    assert e.value.available == size - 1


def test_read_seek():
    src = memory.Backend("r", bytearray(b"0123456789"))
    src.seek(8)
    dst = io.BytesIO()
    with util.aligned_buffer(32) as buf:
        op = ops.Read(src, dst, buf, 5)
        op.run()
    assert dst.getvalue() == b"01234"


def test_read_repr():
    op = ops.Read(None, None, None, 200, offset=24)
    rep = repr(op)
    assert "Read" in rep
    assert "size=200 offset=24 done=0" in rep


@pytest.mark.parametrize("preallocated", [
    pytest.param(True, id="preallocated"),
    pytest.param(False, id="empty"),
])
@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_write_new(user_file, offset, size, preallocated):
    with io.open(user_file.path, "wb") as f:
        if preallocated:
            f.truncate(offset + size)

    src = io.BytesIO(b"x" * size)
    with file.open(user_file.url, "r+") as dst, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Write(dst, src, buf, size, offset=offset)
        op.run()

    with io.open(user_file.path, "rb") as f:
        # Nothing is written before offset.
        assert f.read(offset) == b"\0" * offset

        # All data was written.
        assert f.read(size) == src.getvalue()

        # Writing to unaligned size align file size by padding zeroes.
        file_size = os.path.getsize(user_file.path)
        trailer = file_size - offset - size
        assert file_size % user_file.sector_size == 0
        assert f.read() == b"\0" * trailer


@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_write_inside(user_file, offset, size):
    trailer = 8192
    with io.open(user_file.path, "wb") as f:
        f.truncate(offset + size + trailer)

    src = io.BytesIO(b"x" * size)
    with file.open(user_file.url, "r+") as dst, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Write(dst, src, buf, size, offset=offset)
        op.run()

    with io.open(user_file.path, "rb") as f:
        # Nothing is written before offset.
        assert f.read(offset) == b"\0" * offset

        # All data was written.
        assert f.read(size) == src.getvalue()

        # Nothing was written after offset + size, and file size is not
        # modified.
        assert f.read() == b"\0" * trailer


@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_write_partial_content(user_file, offset, size):
    with io.open(user_file.path, "wb") as f:
        f.truncate(size + offset)

    src = io.BytesIO(b"x" * (size - 1))
    with file.open(user_file.url, "r+") as dst, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Write(dst, src, buf, size, offset=offset)
        with pytest.raises(errors.PartialContent) as e:
            op.run()

    assert e.value.requested == size
    assert e.value.available == size - 1


def test_write_seek():
    dst = memory.Backend("r+", bytearray(b"a" * 10))
    dst.seek(8)
    src = io.BytesIO(b"b" * 5)
    with util.aligned_buffer(32) as buf:
        op = ops.Write(dst, src, buf, 5)
        op.run()
    dst.seek(0)
    b = bytearray(11)
    n = dst.readinto(b)
    assert n == 10
    assert b == b"bbbbbaaaaa\0"


@pytest.mark.parametrize("extra, dirty", [
    ({}, False),  # Flushes by default for backward compatibility.
    ({"flush": True}, False),
    ({"flush": False}, True),
])
def test_write_flush(extra, dirty):
    size = 4096
    dst = memory.Backend("r+", bytearray(b"a" * size))
    src = io.BytesIO(b"b" * size)
    with util.aligned_buffer(4096) as buf:
        op = ops.Write(dst, src, buf, size, **extra)
        op.run()
    assert dst.dirty == dirty


def test_write_repr():
    op = ops.Write(None, None, None, 100, offset=42)
    rep = repr(op)
    assert "Write" in rep
    assert "size=100 offset=42 done=0" in rep


def test_write_unbuffered_stream(user_file):
    chunks = [b"a" * 8192,
              b"b" * 42,
              b"c" * (8192 - 42)]
    src = util.UnbufferedStream(chunks)
    size = sum(len(c) for c in chunks)

    with file.open(user_file.url, "r+") as dst, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Write(dst, src, buf, size)
        op.run()

    with io.open(user_file.path, "rb") as f:
        for c in chunks:
            assert f.read(len(c)) == c
        assert f.read() == b""


def test_write_unbuffered_stream_partial_content(user_file):
    chunks = [b"a" * 8192,
              b"b" * 42,
              b"c" * (8192 - 42)]
    src = util.UnbufferedStream(chunks)
    size = sum(len(c) for c in chunks)

    with file.open(user_file.url, "r+") as dst, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Write(dst, src, buf, size + 1)
        with pytest.raises(errors.PartialContent):
            op.run()


@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_write_no_size(user_file, offset, size):
    with io.open(user_file.path, "wb") as f:
        f.truncate(offset + size)

    src = io.BytesIO(b"x" * size)
    with file.open(user_file.url, "r+") as dst, \
            util.aligned_buffer(1024**2) as buf:
        op = ops.Write(dst, src, buf, offset=offset)
        op.run()

    with io.open(user_file.path, "rb") as f:
        assert f.read(offset) == b"\0" * offset
        assert f.read(size) == src.getvalue()

        file_size = os.path.getsize(user_file.path)
        trailer = file_size - offset - size
        assert file_size % user_file.sector_size == 0
        assert f.read() == b"\0" * trailer


@pytest.mark.parametrize("sparse", [
    pytest.param(True, id="sparse"),
    pytest.param(False, id="preallocated"),
])
@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_zero_full(user_file, offset, size, sparse):
    data = b"x" * (offset + size)
    with io.open(user_file.path, "wb") as f:
        f.write(data)

    with file.open(user_file.url, "r+", sparse=sparse) as dst:
        op = ops.Zero(dst, size, offset=offset)
        op.run()

    with io.open(user_file.path, "rb") as f:
        # Nothing was zeroed before offset
        assert f.read(offset) == data[:offset]

        # Everything was zeroed after offset.
        assert f.read(size) == b"\0" * size

        # Zeroing to unaligned size align file size by padding zereos.
        file_size = os.path.getsize(user_file.path)
        assert file_size % user_file.sector_size == 0
        assert f.read() == b"\0" * (file_size - offset - size)


@pytest.mark.parametrize("sparse", [
    pytest.param(True, id="sparse"),
    pytest.param(False, id="preallocated"),
])
@pytest.mark.parametrize("offset,size", OFFSET_SIZE)
def test_zero_inside(user_file, offset, size, sparse):
    trailer = 8192
    data = b"x" * (offset + size + trailer)
    with io.open(user_file.path, "wb") as f:
        f.write(data)

    with file.open(user_file.url, "r+", sparse=sparse) as dst:
        op = ops.Zero(dst, size, offset=offset)
        op.run()

    with io.open(user_file.path, "rb") as f:
        # Nothing was zeroed before offset
        assert f.read(offset) == data[:offset]

        # Everything was zeroed after offset.
        assert f.read(size) == b"\0" * size

        # Nothing was zeroed after size.
        assert f.read() == data[offset + size:]


def test_zero_seek():
    dst = memory.Backend("r+", bytearray(b"a" * 10))
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
    dst = memory.Backend("r+", bytearray(b"a" * size))
    op = ops.Zero(dst, size, **extra)
    op.run()
    assert dst.dirty == dirty


def test_zero_repr():
    op = ops.Zero(memory.Backend("r+"), 4096)
    rep = repr(op)
    assert "Zero" in rep
    assert "offset=0" in rep
    assert "size=4096" in rep
    assert "done=0" in rep


def test_flush():
    dst = memory.Backend("r+")
    dst.write(b"x")
    op = ops.Flush(dst)
    op.run()
    assert not dst.dirty


def test_flush_repr():
    op = ops.Flush(memory.Backend("r"))
    rep = repr(op)
    assert "Flush" in rep
    assert "done=0" in rep
