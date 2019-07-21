# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import io
import os
import subprocess

from collections import namedtuple
from contextlib import closing

from six.moves import urllib_parse

import pytest
import userstorage

from ovirt_imageio_common import util
from ovirt_imageio_common.backends import file

from . marks import xfail_python3

BACKENDS = userstorage.load_config("../storage.py").BACKENDS

UserFile = namedtuple("UserFile", "path,url,sector_size")


@pytest.fixture(
    params=[
        BACKENDS["file-512-ext2"],
        BACKENDS["file-512-ext4"],
        BACKENDS["file-512-xfs"],
        BACKENDS["file-4k-ext2"],
        BACKENDS["file-4k-ext4"],
        BACKENDS["file-4k-xfs"],
    ],
    ids=str
)
def user_file(request):
    """
    Return a file: url to user storage.
    """
    storage = request.param
    if not storage.exists():
        pytest.xfail("Storage {} is not available".format(storage.name))

    storage.setup()

    yield UserFile(
        path=storage.path,
        url=urllib_parse.urlparse("file:" + storage.path),
        sector_size=storage.sector_size)

    storage.teardown()


def test_debugging_interface(user_file):
    with file.open(user_file.url, "r+") as f:
        assert f.readable()
        assert f.writable()
        assert not f.sparse
        assert f.name == "file"


def test_open_write_only(user_file):
    with file.open(user_file.url, "w") as f, \
            closing(util.aligned_buffer(user_file.sector_size)) as buf:
        assert not f.readable()
        assert f.writable()
        buf.write(b"x" * user_file.sector_size)
        f.write(buf)
    with io.open(user_file.path, "rb") as f:
        assert f.read() == b"x" * user_file.sector_size


def test_open_write_only_truncate(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * user_file.sector_size)
    with file.open(user_file.url, "w") as f:
        pass
    with io.open(user_file.path, "rb") as f:
        assert f.read() == b""


def test_open_read_only(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * user_file.sector_size)
    with file.open(user_file.url, "r") as f, \
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
        missing = urllib_parse.urlparse("file:/no/such/path")
        with file.open(missing, mode):
            pass
    assert e.value.errno == errno.ENOENT


@pytest.mark.parametrize("size", [0, 511, 4097])
def test_block_size_sparse(user_file, size):
    with io.open(user_file.path, "wb") as f:
        f.truncate(size)

    with file.open(user_file.url, "r") as f:
        assert f.block_size == user_file.sector_size

    with io.open(user_file.path, "rb") as f:
        assert f.read(size) == b"\0" * size


@pytest.mark.parametrize("size", [511, 4097])
def test_block_size_preallocated(user_file, size):
    # This is how vdsm preallocates volumes. This uses fallocate() or fallback
    # to writing one byte per block.
    subprocess.check_output(
        ["fallocate", "--posix", "--length", str(size), user_file.path])

    with file.open(user_file.url, "r") as f:
        assert f.block_size == user_file.sector_size

    with io.open(user_file.path, "rb") as f:
        assert f.read(size) == b"\0" * size


@pytest.mark.parametrize("hole_size", [511, 4097])
def test_block_size_hole(user_file, hole_size):
    data_size = 8192 - hole_size

    with io.open(user_file.path, "wb") as f:
        f.seek(hole_size)
        f.write(b"x" * data_size)

    with file.open(user_file.url, "r") as f:
        assert f.block_size == user_file.sector_size

    with io.open(user_file.path, "rb") as f:
        assert f.read(hole_size) == b"\0" * hole_size
        assert f.read(data_size) == b"x" * data_size


@pytest.mark.parametrize("size", [511, 4097])
def test_block_size_allocated(user_file, size):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * size)

    with file.open(user_file.url, "r") as f:
        assert f.block_size == user_file.sector_size

    with io.open(user_file.path, "rb") as f:
        assert f.read() == b"x" * size


def test_readinto(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(user_file.url, "r") as f, \
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

    with file.open(user_file.url, "r") as f, \
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

    with file.open(user_file.url, "r") as f, \
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
    pytest.param(False, id="preallocted", marks=xfail_python3),
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


@xfail_python3
def test_zero_allocate_space(user_file):
    with file.open(user_file.url, "r+", sparse=False) as f:
        f.zero(8192)
    # File system may report more than file size.
    assert os.stat(user_file.path).st_blocks * 512 >= 8192


def test_zero_sparse_deallocate_space(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 8192)
    with file.open(user_file.url, "r+", sparse=True) as f:
        f.zero(8192)
    assert os.stat(user_file.path).st_blocks * 512 < 8192


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


@xfail_python3
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
    size = user_file.sector_size * 2

    with io.open(user_file.path, "wb") as f:
        f.truncate(size)

    with file.open(user_file.url, "r+", sparse=True) as f:
        # Check initial size.
        f.seek(100)
        assert f.size() == size
        assert f.tell() == 100

        # Check that size() updates when file size is modified.
        f.seek(size)
        f.zero(user_file.sector_size)
        assert f.size() == size + user_file.sector_size
