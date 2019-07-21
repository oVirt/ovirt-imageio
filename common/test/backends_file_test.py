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
            closing(util.aligned_buffer(512)) as buf:
        assert not f.readable()
        assert f.writable()
        buf.write(b"x" * 512)
        f.write(buf)
    with io.open(user_file.path, "rb") as f:
        assert f.read() == b"x" * 512


def test_open_write_only_truncate(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 512)
    with file.open(user_file.url, "w") as f:
        pass
    with io.open(user_file.path, "rb") as f:
        assert f.read() == b""


def test_open_read_only(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 512)
    with file.open(user_file.url, "r") as f, \
            closing(util.aligned_buffer(512)) as buf:
        assert f.readable()
        assert not f.writable()
        f.readinto(buf)
        assert buf[:] == b"x" * 512


def test_open_read_write(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 512)
    with file.open(user_file.url, "r+") as f, \
            closing(util.aligned_buffer(512)) as buf:
        assert f.readable()
        assert f.writable()
        f.readinto(buf)
        buf[:] = b"b" * 512
        f.seek(0)
        f.write(buf)
    with io.open(user_file.path, "rb") as f:
        assert f.read() == b"b" * 512


@pytest.mark.parametrize("mode", ["r", "r+"])
def test_open_no_create(mode):
    with pytest.raises(OSError) as e:
        missing = urllib_parse.urlparse("file:/no/such/path")
        with file.open(missing, mode):
            pass
    assert e.value.errno == errno.ENOENT


def test_block_size(user_file):
    with file.open(user_file.url, "r") as f:
        # We don't support yet 4k drives.
        assert f.block_size == 512


def test_readinto(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(user_file.url, "r") as f, \
            closing(util.aligned_buffer(4096)) as buf:
        n = f.readinto(buf)
        assert n == len(buf)
        assert f.tell() == len(buf)
        assert buf[:] == b"a" * 4096


def test_readinto_short_ulinged(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(user_file.url, "r") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        n = f.readinto(buf)
        assert n == 4096
        assert f.tell() == 4096
        assert buf[:4096] == b"a" * 4096
        assert buf[4096:] == b"\0" * 4096


def test_readinto_short_unaligned(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"a" * 42)
    with file.open(user_file.url, "r") as f, \
            closing(util.aligned_buffer(4096)) as buf:
        n = f.readinto(buf)
        assert n == 42
        assert f.tell() == 42
        assert buf[:42] == b"a" * 42
        assert buf[42:] == b"\0" * (4096 - 42)


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
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Write 10 bytes into the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(600)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 610

    with io.open(user_file.path, "rb") as f:
        assert f.read(600) == b"x" * 600
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (1024 - 610)


def test_write_unaligned_offset_inside(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Write 12 bytes into the first block.
    with file.open(user_file.url, "r+") as f:
        f.seek(500)
        n = f.write(b"y" * 100)
        assert n == 12
        assert f.tell() == 512

    with io.open(user_file.path, "rb") as f:
        assert f.read(500) == b"x" * 500
        assert f.read(12) == b"y" * 12
        assert f.read() == b"x" * 512


def test_write_unaligned_offset_at_end(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Write 24 bytes into the last block.
    with file.open(user_file.url, "r+") as f:
        f.seek(1000)
        n = f.write(b"y" * 100)
        assert n == 24
        assert f.tell() == 1024

    with io.open(user_file.path, "rb") as f:
        assert f.read(1000) == b"x" * 1000
        assert f.read() == b"y" * 24


def test_write_unaligned_offset_after_end(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 512)

    with file.open(user_file.url, "r+") as f:
        f.seek(600)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 610

    with io.open(user_file.path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(88) == b"\0" * 88
        assert f.read(10) == b"y" * 10
        assert f.read() == b"\0" * (1024 - 610)


def test_write_unaligned_buffer_slow_path(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Perform slow read-modify-write in the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(512)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 522

    with io.open(user_file.path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (1024 - 522)


def test_write_unaligned_buffer_fast_path(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 4096)

    # Perform short fast write of 6 blocks.
    buf = util.aligned_buffer(3073)
    buf.write(b"y" * 3073)
    with closing(buf):
        with file.open(user_file.url, "r+") as f:
            f.seek(512)
            n = f.write(buf)
            assert n == 3072
            assert f.tell() == 3584

    with io.open(user_file.path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(3072) == b"y" * 3072
        assert f.read() == b"x" * 512


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
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Zero 10 bytes into the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(600)
        n = f.zero(10)
        assert n == 10
        assert f.tell() == 610

    with io.open(user_file.path, "rb") as f:
        assert f.read(600) == b"x" * 600
        assert f.read(10) == b"\0" * 10
        assert f.read() == b"x" * (1024 - 610)


def test_zero_unaligned_offset_inside(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Zero 12 bytes into the first block.
    with file.open(user_file.url, "r+") as f:
        f.seek(500)
        n = f.zero(100)
        assert n == 12
        assert f.tell() == 512

    with io.open(user_file.path, "rb") as f:
        assert f.read(500) == b"x" * 500
        assert f.read(12) == b"\0" * 12
        assert f.read() == b"x" * 512


def test_zero_unaligned_offset_at_end(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Zero 24 bytes into the last block.
    with file.open(user_file.url, "r+") as f:
        f.seek(1000)
        n = f.zero(100)
        assert n == 24
        assert f.tell() == 1024

    with io.open(user_file.path, "rb") as f:
        assert f.read(1000) == b"x" * 1000
        assert f.read() == b"\0" * 24


def test_zero_unaligned_offset_after_end(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 512)

    with file.open(user_file.url, "r+") as f:
        f.seek(600)
        n = f.zero(10)
        assert n == 10
        assert f.tell() == 610

    with io.open(user_file.path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read() == b"\0" * 512


def test_zero_unaligned_buffer_slow_path(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 1024)

    # Perform slow read-modify-write in the second block.
    with file.open(user_file.url, "r+") as f:
        f.seek(512)
        n = f.zero(10)
        assert n == 10
        assert f.tell() == 522

    with io.open(user_file.path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(10) == b"\0" * 10
        assert f.read() == b"x" * 502


@xfail_python3
def test_zero_unaligned_buffer_fast_path(user_file):
    with io.open(user_file.path, "wb") as f:
        f.write(b"x" * 4096)

    # Perform fast short zero of 6 blocks.
    with file.open(user_file.url, "r+") as f:
        f.seek(512)
        n = f.zero(3073)
        assert n == 3072
        assert f.tell() == 3584

    with io.open(user_file.path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(3072) == b"\0" * 3072
        assert f.read() == b"x" * 512


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
    with io.open(user_file.path, "wb") as f:
        f.truncate(1024)
    with file.open(user_file.url, "r+", sparse=True) as f:
        assert f.size() == 1024
        assert f.tell() == 0
        f.zero(2048)
        f.seek(100)
        assert f.size() == 2048
        assert f.tell() == 100
