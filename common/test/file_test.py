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

from contextlib import closing

import pytest

from ovirt_imageio_common import util
from ovirt_imageio_common.backends import file


def test_open_write_only(tmpdir):
    path = str(tmpdir.join("path"))
    with file.open(path, "w") as f, \
            closing(util.aligned_buffer(512)) as buf:
        buf.write(b"x" * 512)
        f.write(buf)
    with io.open(path, "rb") as f:
        assert f.read() == b"x" * 512


def test_open_write_only_truncate(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 512)
    with file.open(path, "w") as f:
        pass
    with io.open(path, "rb") as f:
        assert f.read() == b""


def test_open_read_only(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 512)
    with file.open(path, "r") as f, \
            closing(util.aligned_buffer(512)) as buf:
        f.readinto(buf)
        assert buf[:] == b"x" * 512


def test_open_read_write(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"a" * 512)
    with file.open(path, "r+") as f, \
            closing(util.aligned_buffer(512)) as buf:
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
        with file.open(path, mode):
            pass
    assert e.value.errno == errno.ENOENT


def test_write_unaligned_offset_complete(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 1024)

    # Write 10 bytes into the second block.
    with file.open(path, "r+") as f:
        f.seek(600)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 610

    with io.open(path, "rb") as f:
        assert f.read(600) == b"x" * 600
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (1024 - 610)


def test_write_unaligned_offset_inside(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 1024)

    # Write 12 bytes into the first block.
    with file.open(path, "r+") as f:
        f.seek(500)
        n = f.write(b"y" * 100)
        assert n == 12
        assert f.tell() == 512

    with io.open(path, "rb") as f:
        assert f.read(500) == b"x" * 500
        assert f.read(12) == b"y" * 12
        assert f.read() == b"x" * 512


def test_write_unaligned_offset_at_end(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 1024)

    # Write 24 bytes into the last block.
    with file.open(path, "r+") as f:
        f.seek(1000)
        n = f.write(b"y" * 100)
        assert n == 24
        assert f.tell() == 1024

    with io.open(path, "rb") as f:
        assert f.read(1000) == b"x" * 1000
        assert f.read() == b"y" * 24


def test_write_unaligned_offset_after_end(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 512)

    with file.open(path, "r+") as f:
        f.seek(600)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 610

    with io.open(path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(88) == b"\0" * 88
        assert f.read(10) == b"y" * 10
        assert f.read() == b"\0" * (1024 - 610)


def test_write_unaligned_buffer_slow_path(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 1024)

    # Perform slow read-modify-write in the second block.
    with file.open(path, "r+") as f:
        f.seek(512)
        n = f.write(b"y" * 10)
        assert n == 10
        assert f.tell() == 522

    with io.open(path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(10) == b"y" * 10
        assert f.read() == b"x" * (1024 - 522)


def test_write_unaligned_buffer_fast_path(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 4096)

    # Perform short fast write of 6 blocks.
    buf = util.aligned_buffer(3073)
    buf.write(b"y" * 3073)
    with closing(buf):
        with file.open(path, "r+") as f:
            f.seek(512)
            n = f.write(buf)
            assert n == 3072
            assert f.tell() == 3584

    with io.open(path, "rb") as f:
        assert f.read(512) == b"x" * 512
        assert f.read(3072) == b"y" * 3072
        assert f.read() == b"x" * 512
