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


def test_open_no_direct_read_only(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"x" * 512)
    with file.open(path, "r", direct=False) as f:
        buf = bytearray(512)
        n = f.readinto(buf)
        assert n == 512
        assert buf == b"x" * n


def test_open_no_direct_read_write(tmpdir):
    path = str(tmpdir.join("path"))
    with io.open(path, "wb") as f:
        f.write(b"a" * 512)
    with file.open(path, "r+", direct=False) as f:
        f.write(b"b" * 512)
        f.seek(0)
        buf = bytearray(512)
        n = f.readinto(buf)
        assert n == 512
        assert buf == b"b" * n


def test_open_no_direct_write_only(tmpdir):
    path = str(tmpdir.join("path"))
    with file.open(path, "w", direct=False) as f:
        f.write(b"x" * 512)
    with io.open(path, "rb") as f:
        assert f.read() == b"x" * 512
