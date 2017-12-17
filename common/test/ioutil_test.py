# ovirt-imageio
# Copyright (C) 2017-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import os
import subprocess

from contextlib import closing

import six
import pytest

from ovirt_imageio_common import directio
from ovirt_imageio_common import ioutil

BLOCKSIZE = 4096

requires_root = pytest.mark.skipif(os.geteuid() != 0, reason="Requires root")


@pytest.fixture
def loop_device(tmpdir):
    backing_file = str(tmpdir.join("backing_file"))
    with directio.open(backing_file, "w") as f:
        buf = directio.aligned_buffer(BLOCKSIZE * 3)
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
    with directio.open(loop_device, "r+") as f:
        ioutil.blkzeroout(f.fileno(), 0, BLOCKSIZE)

    with directio.open(loop_device, "r") as f:
        buf = directio.aligned_buffer(BLOCKSIZE * 3)
        with closing(buf):
            f.readinto(buf)
            assert buf[:BLOCKSIZE] == b"\0" * BLOCKSIZE
            assert buf[BLOCKSIZE:] == b"x" * BLOCKSIZE * 2


@requires_root
def test_zeroout_middle(loop_device):
    with directio.open(loop_device, "r+") as f:
        ioutil.blkzeroout(f.fileno(), BLOCKSIZE, BLOCKSIZE)

    with directio.open(loop_device, "r") as f:
        buf = directio.aligned_buffer(BLOCKSIZE * 3)
        with closing(buf):
            f.readinto(buf)
            assert buf[:BLOCKSIZE] == b"x" * BLOCKSIZE
            assert buf[BLOCKSIZE:-BLOCKSIZE] == b"\0" * BLOCKSIZE
            assert buf[-BLOCKSIZE:] == b"x" * BLOCKSIZE


@requires_root
def test_zeroout_end(loop_device):
    with directio.open(loop_device, "r+") as f:
        ioutil.blkzeroout(f.fileno(), BLOCKSIZE * 2, BLOCKSIZE)

    with directio.open(loop_device, "r") as f:
        buf = directio.aligned_buffer(BLOCKSIZE * 3)
        with closing(buf):
            f.readinto(buf)
            assert buf[:-BLOCKSIZE] == b"x" * BLOCKSIZE * 2
            assert buf[-BLOCKSIZE:] == b"\0" * BLOCKSIZE


# Empty zero buffer

@pytest.mark.parametrize("buf", [
    pytest.param(u"", id="unicode"),
    pytest.param("", id="str"),
    pytest.param(b"", id="bytes"),
    pytest.param(bytearray(), id="bytearray"),
])
def test_is_zero_empty(buf):
    assert ioutil.is_zero(buf)


@pytest.mark.skipif(six.PY3, reason="buffer not available")
@pytest.mark.parametrize("buf", [
    pytest.param(u"", id="unicode"),
    pytest.param("", id="str"),
    pytest.param(b"", id="bytes"),
    pytest.param(bytearray(), id="bytearray"),
])
def test_is_zero_empty_buffer(buf):
    assert ioutil.is_zero(buffer(buf))


@pytest.mark.parametrize("buf", [
    pytest.param(b"", id="bytes"),
    pytest.param(bytearray(), id="bytearray"),
])
def test_is_zero_empty_memoryview(buf):
    assert ioutil.is_zero(memoryview(buf))


# Non-empty zero buffer

@pytest.mark.parametrize("buf", [
    pytest.param(u"\u0000" * 512, id="unicode"),
    pytest.param("\0" * 512, id="str"),
    pytest.param(b"\0" * 512, id="bytes"),
    pytest.param(bytearray(512), id="bytearray"),
])
def test_is_zero(buf):
    assert ioutil.is_zero(buf)


@pytest.mark.skipif(six.PY3, reason="buffer not available")
@pytest.mark.parametrize("buf", [
    pytest.param(u"\u0000" * 512, id="unicode"),
    pytest.param("\0" * 512, id="str"),
    pytest.param(b"\0" * 512, id="bytes"),
    pytest.param(bytearray(512), id="bytearray"),
])
def test_is_zero_buffer(buf):
    assert ioutil.is_zero(buffer(buf))


@pytest.mark.parametrize("buf", [
    pytest.param(b"\0" * 512, id="bytes"),
    pytest.param(bytearray(512), id="bytearray"),
])
def test_is_zero_memoryview(buf):
    assert ioutil.is_zero(memoryview(buf))


# Non-zero buffer with non-zero in first 16 bytes

@pytest.mark.parametrize("buf", [
    pytest.param(u"\u0000" * 15 + u"x", id="unicode"),
    pytest.param("\0" * 15 + "x", id="str"),
    pytest.param(b"\0" * 15 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 15 + b"x"), id="bytearray"),
])
def test_is_not_zero_head(buf):
    assert not ioutil.is_zero(buf)


@pytest.mark.skipif(six.PY3, reason="buffer not available")
@pytest.mark.parametrize("buf", [
    pytest.param(u"\u0000" * 15 + u"x", id="unicode"),
    pytest.param("\0" * 15 + "x", id="str"),
    pytest.param(b"\0" * 15 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 15 + b"x"), id="bytearray"),
])
def test_is_not_zero_head_buffer(buf):
    assert not ioutil.is_zero(buffer(buf))


@pytest.mark.parametrize("buf", [
    pytest.param(b"\0" * 15 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 15 + b"x"), id="bytearray"),
])
def test_is_not_zero_head_memoryview(buf):
    assert not ioutil.is_zero(memoryview(buf))


# Non-zero buffer

@pytest.mark.parametrize("buf", [
    pytest.param(u"\u0000" * 511 + u"x", id="unicode"),
    pytest.param("\0" * 511 + "x", id="str"),
    pytest.param(b"\0" * 511 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 511 + b"x"), id="bytearray"),
])
def test_is_not_zero(buf):
    assert not ioutil.is_zero(buf)


@pytest.mark.skipif(six.PY3, reason="buffer not available")
@pytest.mark.parametrize("buf", [
    pytest.param(u"\u0000" * 511 + u"x", id="unicode"),
    pytest.param("\0" * 511 + "x", id="str"),
    pytest.param(b"\0" * 511 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 511 + b"x"), id="bytearray"),
])
def test_is_not_zero_buffer(buf):
    assert not ioutil.is_zero(buffer(buf))


@pytest.mark.parametrize("buf", [
    pytest.param(b"\0" * 511 + b"x", id="bytes"),
    pytest.param(bytearray(b"\0" * 511 + b"x"), id="bytearray"),
])
def test_is_not_zero_memoryview(buf):
    assert not ioutil.is_zero(memoryview(buf))


# Checking mmap

@pytest.fixture
def aligned_buffer():
    buf = directio.aligned_buffer(BLOCKSIZE)
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
