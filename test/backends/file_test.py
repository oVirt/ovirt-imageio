# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import io
import os
import subprocess
import urllib.parse

from contextlib import closing

import pytest
import userstorage

from ovirt_imageio._internal import errors
from ovirt_imageio._internal import extent
from ovirt_imageio._internal import util
from ovirt_imageio._internal.backends import file

from .. import storage

BACKENDS = userstorage.load_config("storage.py").BACKENDS

# Up to kernel 5.5, block size detection for unallocated file was broken only
# on XFS.  Since 5.5 it is broken on all file systems.
KERNEL_VERSION = tuple(map(int, os.uname()[2].split(".")[:2]))


@pytest.fixture(
    params=[
        # backend, can_detect_sector_size.
        (BACKENDS["file-512-ext2"], KERNEL_VERSION < (5, 5)),
        (BACKENDS["file-512-ext4"], KERNEL_VERSION < (5, 5)),
        (BACKENDS["file-512-xfs"], False),
        (BACKENDS["file-4k-ext2"], True),
        (BACKENDS["file-4k-ext4"], True),
        (BACKENDS["file-4k-xfs"], True),
    ],
    ids=lambda x: str(x[0]),
)
def user_file(request):
    with storage.Backend(*request.param) as backend:
        yield backend


def test_debugging_interface(user_file):
    with file.open(user_file.url, "r+") as f:
        assert f.readable()
        assert f.writable()
        assert not f.sparse
        assert f.name == "file"


def test_open_read_only(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * user_file.sector_size)
    with file.open(user_file.url) as f, \
            closing(util.aligned_buffer(user_file.sector_size)) as buf:
        assert f.readable()
        assert not f.writable()
        f.readinto(buf)
        assert buf[:] == b"x" * user_file.sector_size


def test_open_read_write(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * user_file.sector_size)
    with file.open(user_file.url, "r+") as f, \
            closing(util.aligned_buffer(user_file.sector_size)) as buf:
        assert f.readable()
        assert f.writable()
        f.readinto(buf)
        buf[:] = b"b" * user_file.sector_size
        f.seek(0)
        f.write(buf)
    with io.open(user_file.path, "rb") as f:
        assert f.read() == b"b" * user_file.sector_size


@pytest.mark.parametrize("mode", ["r", "r+"])
def test_open_no_create(mode):
    with pytest.raises(OSError) as e:
        missing = urllib.parse.urlparse("file:/no/such/path")
        with file.open(missing, mode):
            pass
    assert e.value.errno == errno.ENOENT


def test_close(tmpurl):
    with file.open(tmpurl, "r+") as b:
        pass

    # Closing twice does nothing.
    b.close()

    # But other oprations should fail.
    error = "Operation on closed backend"

    with closing(util.aligned_buffer(4096)) as buf:
        with pytest.raises(ValueError) as e:
            b.write(buf)
        assert str(e.value) == error

        with pytest.raises(ValueError) as e:
            b.seek(0)
        assert str(e.value) == error

        with pytest.raises(ValueError):
            b.readinto(buf)
        assert str(e.value) == error


@pytest.mark.parametrize("size", [511, 4097])
def test_block_size_sparse(user_file, size):
    with io.open(user_file.path, "wb") as f:
        f.truncate(size)

    with file.open(user_file.url) as f:
        if user_file.can_detect_sector_size:
            assert f.block_size == user_file.sector_size
        else:
            assert f.block_size in (user_file.sector_size, 4096)

    with io.open(user_file.path, "rb") as f:
        assert f.read(size) == b"\0" * size


@pytest.mark.parametrize("size", [511, 4097])
def test_block_size_preallocated(user_file, size):
    # This is how vdsm preallocates volumes. This uses fallocate() or fallback
    # to writing one byte per block.
    subprocess.check_output(
        ["fallocate", "--posix", "--length", str(size), user_file.path])

    with file.open(user_file.url) as f:
        if user_file.can_detect_sector_size:
            assert f.block_size == user_file.sector_size
        else:
            assert f.block_size in (user_file.sector_size, 4096)

    with io.open(user_file.path, "rb") as f:
        assert f.read(size) == b"\0" * size


def test_block_size_first_block_unallocated(user_file):
    # Assumes file system block size <= 4096.
    hole_size = 4096
    data_size = 4096

    with io.open(user_file.path, "wb") as f:
        f.seek(hole_size)
        f.write(b"x" * data_size)

    with file.open(user_file.url) as f:
        if user_file.can_detect_sector_size:
            assert f.block_size == user_file.sector_size
        else:
            assert f.block_size in (user_file.sector_size, 4096)

    with io.open(user_file.path, "rb") as f:
        assert f.read(hole_size) == b"\0" * hole_size
        assert f.read(data_size) == b"x" * data_size


@pytest.mark.parametrize("size", [511, 4097])
def test_block_size_allocated(user_file, size):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    with file.open(user_file.url) as f:
        assert f.block_size == user_file.sector_size

    with io.open(user_file.path, "rb") as f:
        assert f.read() == b"x" * size


def test_readinto(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(user_file.url) as f, \
            closing(util.aligned_buffer(4096)) as buf:
        n = f.readinto(buf)
        assert n == len(buf)
        assert f.tell() == len(buf)
        assert buf[:] == b"a" * 4096


def test_readinto_short_aligned(user_file):
    size = user_file.sector_size
    buf_size = user_file.sector_size * 2

    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * size)

    with file.open(user_file.url) as f, \
            closing(util.aligned_buffer(buf_size)) as buf:
        n = f.readinto(buf)
        assert n == size
        assert f.tell() == size
        assert buf[:size] == b"a" * size
        assert buf[size:] == b"\0" * (buf_size - size)


def test_readinto_short_unaligned(user_file):
    size = 42
    buf_size = user_file.sector_size

    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * size)

    with file.open(user_file.url) as f, \
            closing(util.aligned_buffer(buf_size)) as buf:
        n = f.readinto(buf)
        assert n == size
        assert f.tell() == size
        assert buf[:size] == b"a" * size
        assert buf[size:] == b"\0" * (buf_size - size)


def test_write_aligned_middle(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 4 * 4096)
    with file.open(user_file.url, "r+") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        buf.write(b"b" * 8192)
        f.seek(4096)
        n = f.write(buf)
        assert n == len(buf)
        assert f.tell() == 4096 + len(buf)
    with io.open(user_file.path, "rb") as f:
        assert f.read(4096) == b"a" * 4096
        assert f.read(8192) == b"b" * 8192
        assert f.read() == b"a" * 4096


def test_write_aligned_at_end(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 8192)
    with file.open(user_file.url, "r+") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        buf.write(b"b" * 8192)
        f.seek(4096)
        n = f.write(buf)
        assert n == len(buf)
        assert f.tell() == 4096 + len(buf)
    with io.open(user_file.path, "rb") as f:
        assert f.read(4096) == b"a" * 4096
        assert f.read() == b"b" * 8192


def test_write_aligned_after_end(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(user_file.url, "r+") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        buf.write(b"b" * 8192)
        f.seek(8192)
        n = f.write(buf)
        assert n == len(buf)
        assert f.tell() == 8192 + len(buf)
    with io.open(user_file.path, "rb") as f:
        assert f.read(4096) == b"a" * 4096
        assert f.read(4096) == b"\0" * 4096
        assert f.read() == b"b" * 8192


def test_write_unaligned_offset_complete(user_file):
    size = user_file.sector_size * 2
    start = user_file.sector_size + 10
    end = start + 10

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Write 10 bytes into the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == end

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (size - end)


def test_write_unaligned_offset_inside(user_file):
    size = user_file.sector_size * 2
    start = user_file.sector_size - 12

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Write 12 bytes into the first block.
    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.write(b"y" * 100)
        assert n == 12
        assert f.tell() == user_file.sector_size

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read(12) == b"y" * 12
        assert f.read() == b"x" * user_file.sector_size


def test_write_unaligned_offset_at_end(user_file):
    size = user_file.sector_size * 2
    start = size - 24

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Write 24 bytes into the last block.
    with file.open(user_file.url, "r+") as f:
        f.seek(size - 24)
        n = f.write(b"y" * 100)
        assert n == 24
        assert f.tell() == size

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read() == b"y" * 24


def test_write_unaligned_offset_after_end(user_file):
    size = user_file.sector_size

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    with file.open(user_file.url, "r+") as f:
        f.seek(size + 10)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == size + 20

    with io.open(user_file.path, "rb") as f:
        assert f.read(size) == b"x" * size
        assert f.read(10) == b"\0" * 10
        assert f.read(10) == b"y" * 10
        assert f.read() == b"\0" * (user_file.sector_size - 20)


def test_write_unaligned_buffer_slow_path(user_file):
    size = user_file.sector_size * 2

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Perform slow read-modify-write in the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(user_file.sector_size)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == user_file.sector_size + 10

    with io.open(user_file.path, "rb") as f:
        assert f.read(user_file.sector_size) == b"x" * user_file.sector_size
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (user_file.sector_size - 10)


def test_write_unaligned_buffer_fast_path(user_file):
    size = user_file.sector_size * 4
    start = user_file.sector_size
    length = user_file.sector_size * 2

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Create buffer of 2 blcoks + 1 byte.
    buf = util.aligned_buffer(length + 1)
    buf.write(b"y" * len(buf))

    # Perform short fast write of 2 blocks.
    with closing(buf):
        with file.open(user_file.url, "r+") as f:
            f.seek(start)
            n = f.write(buf)
            assert n == length
            assert f.tell() == start + length

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read(length) == b"y" * length
        assert f.read() == b"x" * user_file.sector_size


def test_flush(user_file, monkeypatch):
    count = [0]

    def fsync(fd):
        count[0] += 1

    # This is ugly but probably the only way to test that we call fsync.
    monkeypatch.setattr(os, "fsync", fsync)
    with file.open(user_file.url, "r+") as f:
        f.write(b"x")
        f.flush()
    assert count[0] == 1


ZERO_SPARSE = [
    pytest.param(True, id="sparse"),
    pytest.param(False, id="preallocted"),
]


@pytest.mark.parametrize("sparse", ZERO_SPARSE)
def test_zero_aligned_middle(user_file, sparse):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 3 * 4096)
    with file.open(user_file.url, "r+", sparse=sparse) as f:
        f.seek(4096)
        n = f.zero(4096)
        assert n == 4096
        assert f.tell() == 8192
    with io.open(user_file.path, "rb") as f:
        assert f.read(4096) == b"x" * 4096
        assert f.read(4096) == b"\0" * 4096
        assert f.read() == b"x" * 4096


@pytest.mark.parametrize("sparse", ZERO_SPARSE)
def test_zero_aligned_at_end(user_file, sparse):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 4096)
    with file.open(user_file.url, "r+", sparse=sparse) as f:
        assert f.sparse == sparse
        f.seek(4096)
        n = f.zero(4096)
        assert n == 4096
        assert f.tell() == 8192
    with io.open(user_file.path, "rb") as f:
        assert f.read(4096) == b"x" * 4096
        assert f.read() == b"\0" * 4096


@pytest.mark.parametrize("sparse", ZERO_SPARSE)
def test_zero_aligned_after_end(user_file, sparse):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 4096)
    with file.open(user_file.url, "r+", sparse=sparse) as f:
        f.seek(8192)
        n = f.zero(4096)
        assert n == 4096
        assert f.tell() == 12288
    with io.open(user_file.path, "rb") as f:
        assert f.read(4096) == b"x" * 4096
        assert f.read() == b"\0" * 8192


def test_zero_allocate_space(user_file):
    size = 1024**2
    with file.open(user_file.url, "r+", sparse=False) as f:
        f.zero(size)
    # File system may report more than file size.
    assert os.stat(user_file.path).st_blocks * 512 >= size


def test_zero_sparse_deallocate_space(user_file):
    size = 1024**2
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)
    with file.open(user_file.url, "r+", sparse=True) as f:
        f.zero(size)
    assert os.stat(user_file.path).st_blocks * 512 < size


def test_zero_unaligned_offset_complete(user_file):
    size = user_file.sector_size * 2
    start = user_file.sector_size + 10

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Zero 10 bytes into the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.zero(10)
        assert n == 10
        assert f.tell() == start + 10

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read(10) == b"\0" * 10
        assert f.read() == b"x" * (size - start - 10)


def test_zero_unaligned_offset_inside(user_file):
    size = user_file.sector_size * 2
    start = user_file.sector_size - 10

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Zero 10 bytes into the first block.
    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.zero(100)
        assert n == 10
        assert f.tell() == start + 10

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read(10) == b"\0" * 10
        assert f.read() == b"x" * (size - start - 10)


def test_zero_unaligned_offset_at_end(user_file):
    size = user_file.sector_size * 2
    start = size - 10

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Zero 10 bytes into the last block.
    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.zero(100)
        assert n == 10
        assert f.tell() == size

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read() == b"\0" * 10


def test_zero_unaligned_offset_after_end(user_file):
    size = user_file.sector_size
    start = size + 10

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.zero(10)
        assert n == 10
        assert f.tell() == start + 10

    with io.open(user_file.path, "rb") as f:
        assert f.read(size) == b"x" * size
        assert f.read() == b"\0" * user_file.sector_size


def test_zero_unaligned_buffer_slow_path(user_file):
    size = user_file.sector_size * 2
    start = user_file.sector_size

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Perform slow read-modify-write in the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.zero(10)
        assert n == 10
        assert f.tell() == start + 10

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read(10) == b"\0" * 10
        assert f.read() == b"x" * (size - start - 10)


def test_zero_unaligned_buffer_fast_path(user_file):
    size = user_file.sector_size * 4
    start = user_file.sector_size
    length = user_file.sector_size * 2

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    # Perform fast short zero of 2 blocks.
    with file.open(user_file.url, "r+") as f:
        f.seek(start)
        n = f.zero(length + 1)
        assert n == length
        assert f.tell() == start + length

    with io.open(user_file.path, "rb") as f:
        assert f.read(start) == b"x" * start
        assert f.read(length) == b"\0" * length
        assert f.read() == b"x" * (size - start - length)


def test_dirty(user_file):
    # backend created clean
    with file.open(user_file.url, "r+", sparse=True) as f:
        assert not f.dirty
        buf = util.aligned_buffer(4096)
        with closing(buf):
            # write ans zero dirty the backend
            buf.write(b"x" * 4096)
            f.write(buf)
            assert f.dirty
            f.flush()
            assert not f.dirty
            f.zero(4096)
            assert f.dirty
            f.flush()
            assert not f.dirty

            # readinto, seek do not affect dirty.
            f.seek(0)
            assert not f.dirty
            f.readinto(buf)
            assert not f.dirty


def test_size(user_file):
    size = 4096

    with io.open(user_file.path, "wb") as f:
        f.truncate(size)

    with file.open(user_file.url, "r+", sparse=True) as f:
        # Check initial size.
        f.seek(100)
        assert f.size() == size
        assert f.tell() == 100

        # Check that size() updates when file size is modified.
        f.seek(size)
        f.zero(4096)
        assert f.size() == 8192


def test_extents(user_file):
    size = user_file.sector_size * 2

    with io.open(user_file.path, "wb") as f:
        f.truncate(size)

    with file.open(user_file.url, "r+", sparse=True) as f:
        # We support detecting extents now; empty file reports one data
        # extents.
        assert list(f.extents()) == [
            extent.ZeroExtent(0, size, False, False)
        ]


def test_extents_dirty(user_file):
    with file.open(user_file.url, "r+", dirty=True) as f:
        with pytest.raises(errors.UnsupportedOperation):
            list(f.extents(context="dirty"))


def test_clone(user_file):
    size = user_file.sector_size * 2

    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    with file.open(user_file.url, "r+", sparse=True) as a, \
            a.clone() as b, \
            util.aligned_buffer(user_file.sector_size) as buf:

        # Backends are identical when created.
        assert a.size() == b.size()
        assert a.tell() == b.tell()
        assert a.block_size == b.block_size

        # Operations on one backend do not affect the other.
        buf[:] = b"y" * len(buf)
        a.write(buf)
        assert a.tell() == len(buf)
        assert b.tell() == 0

        # But both expose the same file contents.
        buf[:] = b"\0" * len(buf)
        b.readinto(buf)
        assert buf[:] == b"y" * len(buf)
