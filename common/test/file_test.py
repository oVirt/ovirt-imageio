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

from contextlib import closing

import pytest

from ovirt_imageio_common import util
from ovirt_imageio_common.backends import file


def test_open_write_only(tmpfile):
    with file.open(tmpfile, "w") as f, \
            closing(util.aligned_buffer(512)) as buf:
        buf.write(b"x" * 512)
        f.write(buf)
    with io.open(tmpfile, "rb") as f:
        assert f.read() == b"x" * 512


def test_open_write_only_truncate(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 512)
    with file.open(tmpfile, "w") as f:
        pass
    with io.open(tmpfile, "rb") as f:
        assert f.read() == b""


def test_open_read_only(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 512)
    with file.open(tmpfile, "r") as f, \
            closing(util.aligned_buffer(512)) as buf:
        f.readinto(buf)
        assert buf[:] == b"x" * 512


def test_open_read_write(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"a" * 512)
    with file.open(tmpfile, "r+") as f, \
            closing(util.aligned_buffer(512)) as buf:
        f.readinto(buf)
        buf[:] = b"b" * 512
        f.seek(0)
        f.write(buf)
    with io.open(tmpfile, "rb") as f:
        assert f.read() == b"b" * 512


@pytest.mark.parametrize("mode", ["r", "r+"])
def test_open_no_create(mode):
    with pytest.raises(OSError) as e:
        with file.open("/no/such/path", mode):
            pass
    assert e.value.errno == errno.ENOENT


def test_readinto(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(tmpfile, "r") as f, \
            closing(util.aligned_buffer(4096)) as buf:
        n = f.readinto(buf)
        assert n == len(buf)
        assert f.tell() == len(buf)
        assert buf[:] == b"a" * 4096


def test_readinto_short_ulinged(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(tmpfile, "r") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        n = f.readinto(buf)
        assert n == 4096
        assert f.tell() == 4096
        assert buf[:4096] == b"a" * 4096
        assert buf[4096:] == b"\0" * 4096


def test_readinto_short_unaligned(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"a" * 42)
    with file.open(tmpfile, "r") as f, \
            closing(util.aligned_buffer(4096)) as buf:
        n = f.readinto(buf)
        assert n == 42
        assert f.tell() == 42
        assert buf[:42] == b"a" * 42
        assert buf[42:] == b"\0" * (4096 - 42)


def test_write_aligned_middle(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"a" * 4 * 4096)
    with file.open(tmpfile, "r+") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        buf.write(b"b" * 8192)
        f.seek(4096)
        n = f.write(buf)
        assert n == len(buf)
        assert f.tell() == 4096 + len(buf)
    with io.open(tmpfile, "rb") as f:
        assert f.read(4096) == b"a" * 4096
        assert f.read(8192) == b"b" * 8192
        assert f.read() == b"a" * 4096


def test_write_aligned_at_end(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"a" * 8192)
    with file.open(tmpfile, "r+") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        buf.write(b"b" * 8192)
        f.seek(4096)
        n = f.write(buf)
        assert n == len(buf)
        assert f.tell() == 4096 + len(buf)
    with io.open(tmpfile, "rb") as f:
        assert f.read(4096) == b"a" * 4096
        assert f.read() == b"b" * 8192


def test_write_aligned_after_end(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"a" * 4096)
    with file.open(tmpfile, "r+") as f, \
            closing(util.aligned_buffer(8192)) as buf:
        buf.write(b"b" * 8192)
        f.seek(8192)
        n = f.write(buf)
        assert n == len(buf)
        assert f.tell() == 8192 + len(buf)
    with io.open(tmpfile, "rb") as f:
        assert f.read(4096) == b"a" * 4096
        assert f.read(4096) == b"\0" * 4096
        assert f.read() == b"b" * 8192


def test_write_unaligned_offset_complete(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 1024)

    # Write 10 bytes into the second block.
    with file.open(tmpfile, "r+") as f:
        f.seek(600)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 610

    with io.open(tmpfile, "rb") as f:
        assert f.read(600) == b"x" * 600
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (1024 - 610)


def test_write_unaligned_offset_inside(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 1024)

    # Write 12 bytes into the first block.
    with file.open(tmpfile, "r+") as f:
        f.seek(500)
        n = f.write(b"y" * 100)
        assert n == 12
        assert f.tell() == 512

    with io.open(tmpfile, "rb") as f:
        assert f.read(500) == b"x" * 500
        assert f.read(12) == b"y" * 12
        assert f.read() == b"x" * 512


def test_write_unaligned_offset_at_end(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 1024)

    # Write 24 bytes into the last block.
    with file.open(tmpfile, "r+") as f:
        f.seek(1000)
        n = f.write(b"y" * 100)
        assert n == 24
        assert f.tell() == 1024

    with io.open(tmpfile, "rb") as f:
        assert f.read(1000) == b"x" * 1000
        assert f.read() == b"y" * 24


def test_write_unaligned_offset_after_end(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 512)

    with file.open(tmpfile, "r+") as f:
        f.seek(600)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 610

    with io.open(tmpfile, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(88) == b"\0" * 88
        assert f.read(10) == b"y" * 10
        assert f.read() == b"\0" * (1024 - 610)


def test_write_unaligned_buffer_slow_path(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 1024)

    # Perform slow read-modify-write in the second block.
    with file.open(tmpfile, "r+") as f:
        f.seek(512)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 522

    with io.open(tmpfile, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (1024 - 522)


def test_write_unaligned_buffer_fast_path(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 4096)

    # Perform short fast write of 6 blocks.
    buf = util.aligned_buffer(3073)
    buf.write(b"y" * 3073)
    with closing(buf):
        with file.open(tmpfile, "r+") as f:
            f.seek(512)
            n = f.write(buf)
            assert n == 3072
            assert f.tell() == 3584

    with io.open(tmpfile, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(3072) == b"y" * 3072
        assert f.read() == b"x" * 512


def test_flush(tmpfile, monkeypatch):
    count = [0]

    def fsync(fd):
        count[0] += 1

    # This is ugly but probably the only way to test that we call fsync.
    monkeypatch.setattr(os, "fsync", fsync)
    with file.open(tmpfile, "r+") as f:
        f.write(b"x")
        f.flush()
    assert count[0] == 1


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_aligned_middle(tmpfile, sparse):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 3 * 4096)
    with file.open(tmpfile, "r+", sparse=sparse) as f:
        f.seek(4096)
        n = f.zero(4096)
        assert n == 4096
        assert f.tell() == 8192
    with io.open(tmpfile, "rb") as f:
        assert f.read(4096) == b"x" * 4096
        assert f.read(4096) == b"\0" * 4096
        assert f.read() == b"x" * 4096


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_aligned_at_end(tmpfile, sparse):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 4096)
    with file.open(tmpfile, "r+", sparse=sparse) as f:
        f.seek(4096)
        n = f.zero(4096)
        assert n == 4096
        assert f.tell() == 8192
    with io.open(tmpfile, "rb") as f:
        assert f.read(4096) == b"x" * 4096
        assert f.read() == b"\0" * 4096


@pytest.mark.parametrize("sparse", [True, False])
def test_zero_aligned_after_end(tmpfile, sparse):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 4096)
    with file.open(tmpfile, "r+", sparse=sparse) as f:
        f.seek(8192)
        n = f.zero(4096)
        assert n == 4096
        assert f.tell() == 12288
    with io.open(tmpfile, "rb") as f:
        assert f.read(4096) == b"x" * 4096
        assert f.read() == b"\0" * 8192


def test_zero_allocate_space(tmpfile):
    with file.open(tmpfile, "r+", sparse=False) as f:
        f.zero(8192)
    # File system may report more than file size.
    assert os.stat(tmpfile).st_blocks * 512 >= 8192


def test_zero_sparse_deallocate_space(tmpfile):
    with io.open(tmpfile, "wb") as f:
        f.write(b"x" * 8192)
    with file.open(tmpfile, "r+", sparse=True) as f:
        f.zero(8192)
    assert os.stat(tmpfile).st_blocks * 512 < 8192
