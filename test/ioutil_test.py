# ovirt-imageio
# Copyright (C) 2017-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import os
import subprocess

from contextlib import closing

import pytest

from ovirt_imageio._internal import ioutil
from ovirt_imageio._internal import util

BLOCKSIZE = 4096

requires_root = pytest.mark.skipif(os.geteuid() != 0, reason="Requires root")


@pytest.fixture
def loop_device(tmpdir):
    backing_file = str(tmpdir.join("backing_file"))
    with util.open(backing_file, "w") as f:
        buf = util.aligned_buffer(BLOCKSIZE * 3)
        with closing(buf):
            buf[:] = b"x" * BLOCKSIZE * 3
            f.write(buf)
    out = subprocess.check_output(
        ["losetup", "--find", backing_file, "--show"])
    try:
        loop = out.strip().decode("ascii")
        yield loop
    finally:
        subprocess.check_call(["losetup", "--detach", loop])


@requires_root
def test_zeroout_start(loop_device):
    with util.open(loop_device, "r+") as f:
        ioutil.blkzeroout(f.fileno(), 0, BLOCKSIZE)

    with util.open(loop_device, "r") as f:
        buf = util.aligned_buffer(BLOCKSIZE * 3)
        with closing(buf):
            f.readinto(buf)
            assert buf[:BLOCKSIZE] == b"\0" * BLOCKSIZE
            assert buf[BLOCKSIZE:] == b"x" * BLOCKSIZE * 2


@requires_root
def test_zeroout_middle(loop_device):
    with util.open(loop_device, "r+") as f:
        ioutil.blkzeroout(f.fileno(), BLOCKSIZE, BLOCKSIZE)

    with util.open(loop_device, "r") as f:
        buf = util.aligned_buffer(BLOCKSIZE * 3)
        with closing(buf):
            f.readinto(buf)
            assert buf[:BLOCKSIZE] == b"x" * BLOCKSIZE
            assert buf[BLOCKSIZE:-BLOCKSIZE] == b"\0" * BLOCKSIZE
            assert buf[-BLOCKSIZE:] == b"x" * BLOCKSIZE


@requires_root
def test_zeroout_end(loop_device):
    with util.open(loop_device, "r+") as f:
        ioutil.blkzeroout(f.fileno(), BLOCKSIZE * 2, BLOCKSIZE)

    with util.open(loop_device, "r") as f:
        buf = util.aligned_buffer(BLOCKSIZE * 3)
        with closing(buf):
            f.readinto(buf)
            assert buf[:-BLOCKSIZE] == b"x" * BLOCKSIZE * 2
            assert buf[-BLOCKSIZE:] == b"\0" * BLOCKSIZE


@requires_root
def test_blksszget_512(loop_device):
    with util.open(loop_device, "r+") as f:
        assert ioutil.blksszget(f.fileno()) == 512


@requires_root
def test_blksszget_bad_fd(loop_device):
    with pytest.raises(OSError):
        ioutil.blksszget(-1)


@requires_root
def test_blksszget_not_block_device(loop_device, tmpfile):
    with open(tmpfile) as f:
        with pytest.raises(OSError):
            ioutil.blksszget(f.fileno())


# Empty zero buffer

@pytest.mark.parametrize("buf", [
    pytest.param("", id="unicode"),
    pytest.param(b"", id="bytes"),
    pytest.param(bytearray(), id="bytearray"),
])
def test_is_zero_empty(buf):
    assert ioutil.is_zero(buf)


@pytest.mark.parametrize("buf", [
    pytest.param(b"", id="bytes"),
    pytest.param(bytearray(), id="bytearray"),
])
def test_is_zero_empty_memoryview(buf):
    assert ioutil.is_zero(memoryview(buf))


# Non-empty zero buffer

@pytest.mark.parametrize("buf", [
    pytest.param("\u0000" * 512, id="unicode"),
    pytest.param(b"\0" * 512, id="bytes"),
    pytest.param(bytearray(512), id="bytearray"),
])
def test_is_zero(buf):
    assert ioutil.is_zero(buf)


@pytest.mark.parametrize("buf", [
    pytest.param(b"\0" * 512, id="bytes"),
    pytest.param(bytearray(512), id="bytearray"),
])
def test_is_zero_memoryview(buf):
    assert ioutil.is_zero(memoryview(buf))


# Non-zero buffer with non-zero in first 16 bytes

@pytest.mark.parametrize("buf", [
    pytest.param("\u0000" * 15 + "x", id="unicode"),
    pytest.param(b"\0" * 15 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 15 + b"x"), id="bytearray"),
])
def test_is_not_zero_head(buf):
    assert not ioutil.is_zero(buf)


@pytest.mark.parametrize("buf", [
    pytest.param(b"\0" * 15 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 15 + b"x"), id="bytearray"),
])
def test_is_not_zero_head_memoryview(buf):
    assert not ioutil.is_zero(memoryview(buf))


# Non-zero buffer

@pytest.mark.parametrize("buf", [
    pytest.param("\u0000" * 511 + "x", id="unicode"),
    pytest.param(b"\0" * 511 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 511 + b"x"), id="bytearray"),
])
def test_is_not_zero(buf):
    assert not ioutil.is_zero(buf)


@pytest.mark.parametrize("buf", [
    pytest.param(b"\0" * 511 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 511 + b"x"), id="bytearray"),
])
def test_is_not_zero_memoryview(buf):
    assert not ioutil.is_zero(memoryview(buf))


# Checking mmap

@pytest.fixture
def aligned_buffer():
    buf = util.aligned_buffer(BLOCKSIZE)
    with closing(buf):
        yield buf


def test_is_zero_mmap(aligned_buffer):
    assert ioutil.is_zero(aligned_buffer)


def test_is_not_zero_mmap_head(aligned_buffer):
    aligned_buffer[15:16] = b"x"
    assert not ioutil.is_zero(aligned_buffer)


def test_is_not_zero_mmap(aligned_buffer):
    aligned_buffer[-1:] = b"x"
    assert not ioutil.is_zero(aligned_buffer)


# fallocate

fallocate_mode = pytest.mark.parametrize("mode", [
    # Zero byte range, allocating space - for preallocated images.
    # Not supported on NFS 4.2.
    ioutil.FALLOC_FL_ZERO_RANGE,
    # Zero byte range, daallocating space - for sparse images.
    # Supported on NFS 4.2.
    ioutil.FALLOC_FL_PUNCH_HOLE | ioutil.FALLOC_FL_KEEP_SIZE,
])


@fallocate_mode
def test_fallocate_zero_start(tmpdir, mode):
    path = str(tmpdir.join("file"))
    with open(path, "wb") as f:
        f.write(b"x" * BLOCKSIZE * 3)

    buf = util.aligned_buffer(BLOCKSIZE * 3)
    with closing(buf), util.open(path, "r+") as f:
        try_fallocate(f.fileno(), mode, 0, BLOCKSIZE)

        n = f.readinto(buf)
        assert n == BLOCKSIZE * 3
        assert buf[:BLOCKSIZE] == b"\0" * BLOCKSIZE
        assert buf[BLOCKSIZE:] == b"x" * BLOCKSIZE * 2
        assert f.readinto(buf) == 0


@fallocate_mode
def test_fallocate_zero_middle(tmpdir, mode):
    path = str(tmpdir.join("file"))
    with open(path, "wb") as f:
        f.write(b"x" * BLOCKSIZE * 3)

    buf = util.aligned_buffer(BLOCKSIZE * 3)
    with closing(buf), util.open(path, "r+") as f:
        try_fallocate(f.fileno(), mode, BLOCKSIZE, BLOCKSIZE)

        n = f.readinto(buf)
        assert n == BLOCKSIZE * 3
        assert buf[:BLOCKSIZE] == b"x" * BLOCKSIZE
        assert buf[BLOCKSIZE:-BLOCKSIZE] == b"\0" * BLOCKSIZE
        assert buf[-BLOCKSIZE:] == b"x" * BLOCKSIZE
        assert f.readinto(buf) == 0


@fallocate_mode
def test_fallocate_zero_end(tmpdir, mode):
    path = str(tmpdir.join("file"))
    with open(path, "wb") as f:
        f.write(b"x" * BLOCKSIZE * 3)

    buf = util.aligned_buffer(BLOCKSIZE * 3)
    with closing(buf), util.open(path, "r+") as f:
        try_fallocate(f.fileno(), mode, BLOCKSIZE * 2, BLOCKSIZE)

        n = f.readinto(buf)
        assert n == BLOCKSIZE * 3
        assert buf[:-BLOCKSIZE] == b"x" * BLOCKSIZE * 2
        assert buf[-BLOCKSIZE:] == b"\0" * BLOCKSIZE
        assert f.readinto(buf) == 0


def test_fallocate_zero_after_end(tmpdir):
    path = str(tmpdir.join("file"))
    with open(path, "wb") as f:
        f.write(b"x" * BLOCKSIZE * 3)

    buf = util.aligned_buffer(BLOCKSIZE * 4)
    with closing(buf), util.open(path, "r+") as f:
        # Will allocate more space that will return zeros when read.
        mode = ioutil.FALLOC_FL_ZERO_RANGE
        try_fallocate(f.fileno(), mode, BLOCKSIZE * 3, BLOCKSIZE)

        n = f.readinto(buf)
        assert n == BLOCKSIZE * 4
        assert buf[:-BLOCKSIZE] == b"x" * BLOCKSIZE * 3
        assert buf[-BLOCKSIZE:] == b"\0" * BLOCKSIZE
        assert f.readinto(buf) == 0


def test_fallocate_punch_hole_after_end(tmpdir):
    path = str(tmpdir.join("file"))
    with open(path, "wb") as f:
        f.write(b"x" * BLOCKSIZE * 3)

    buf = util.aligned_buffer(BLOCKSIZE * 3)
    with closing(buf), util.open(path, "r+") as f:
        # This does not change file contents or size.
        mode = ioutil.FALLOC_FL_PUNCH_HOLE | ioutil.FALLOC_FL_KEEP_SIZE
        try_fallocate(f.fileno(), mode, BLOCKSIZE * 3, BLOCKSIZE)

        n = f.readinto(buf)
        assert n == BLOCKSIZE * 3
        assert buf[:] == b"x" * BLOCKSIZE * 3
        assert f.readinto(buf) == 0


def try_fallocate(fd, mode, offset, count):
    try:
        ioutil.fallocate(fd, mode, offset, count)
    except EnvironmentError as e:
        if e.errno != errno.EOPNOTSUPP:
            raise
        pytest.skip("fallocate(mode=%r) not supported" % mode)
