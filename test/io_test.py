# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import time
import pytest

from urllib.parse import urlparse

from ovirt_imageio._internal import extent
from ovirt_imageio._internal import io
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal.backends import nbd, memory
from ovirt_imageio._internal.nbd import UnixAddress

ZERO_PARAMS = [
    # Copying to image with unknown content.
    pytest.param(True, True, id="unknown-image"),

    # Copying to new empty image.
    pytest.param(False, True, id="new-image"),

    # Copying to new empty image with a backing file.
    pytest.param(True, False, id="backing-file"),
]

CHUNK_SIZE = 512


class FakeProgress:

    def __init__(self):
        self.updates = []

    def update(self, n):
        self.updates.append(n)


@pytest.mark.parametrize("src_fmt,dst_fmt", [
    ("raw", "raw"),
    ("raw", "qcow2"),
    ("qcow2", "qcow2"),
    ("qcow2", "raw"),
])
@pytest.mark.parametrize("zero,hole", ZERO_PARAMS)
def test_copy_nbd_to_nbd(tmpdir, src_fmt, dst_fmt, zero, hole):
    # Default cluser size with qcow2 format.
    cluster_size = 64 * 1024
    extents = [
        ("data", cluster_size),
        ("zero", cluster_size),
        ("data", cluster_size),
        ("hole", cluster_size + io.MAX_ZERO_SIZE),
        ("data", cluster_size + io.BUFFER_SIZE),
        ("hole", cluster_size),
        ("data", cluster_size),
    ]
    size = sum(length for _, length in extents)

    src = str(tmpdir.join("src." + src_fmt))
    qemu_img.create(src, src_fmt, size=size)
    populate_image(src, src_fmt, extents)

    src_sock = UnixAddress(tmpdir.join("src.sock"))
    src_url = urlparse(src_sock.url())

    dst = str(tmpdir.join("dst." + dst_fmt))
    qemu_img.create(dst, dst_fmt, size=size)
    dst_sock = UnixAddress(tmpdir.join("dst.sock"))
    dst_url = urlparse(dst_sock.url())

    # Note: We need extra worker for reading extents for source.
    max_workers = 2
    with qemu_nbd.run(
                src, src_fmt, src_sock,
                read_only=True,
                shared=max_workers + 1), \
            qemu_nbd.run(
                dst, dst_fmt, dst_sock,
                shared=max_workers), \
            nbd.open(src_url, "r") as src_backend, \
            nbd.open(dst_url, "r+", sparse=True) as dst_backend:

        # Because we copy to new image, we can always use zero=False, but we
        # test both to verify that the result is the same.
        io.copy(
            src_backend,
            dst_backend,
            max_workers=max_workers,
            zero=zero,
            hole=hole)

    # Compare image content - must match.
    qemu_img.compare(src, dst)

    # Allocation can be compared only with qcow2 images when we write zeroes to
    # zero extents and skip holes.
    if src_fmt == "qcow2" and dst_fmt == "qcow2" and zero and not hole:
        qemu_img.compare(src, dst, strict=True)


@pytest.mark.parametrize("buffer_size", [128, 1024])
@pytest.mark.parametrize("zero,hole", ZERO_PARAMS)
@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_generic(buffer_size, zero, hole, progress):
    src_extents = create_zero_extents("B0-")
    src_backing = create_backing("B0-")

    dst_backing = create_backing(
        "AAA" if zero and hole else "AA0" if zero else "A00")

    src = memory.Backend(
        mode="r", data=src_backing, extents={"zero": src_extents})

    dst = memory.Backend("r+", data=dst_backing)

    io.copy(
        src, dst,
        max_workers=1,
        buffer_size=buffer_size,
        zero=zero,
        hole=hole,
        progress=progress)

    assert dst_backing == src_backing


@pytest.mark.parametrize("buffer_size", [128, 1024])
@pytest.mark.parametrize("zero,hole", ZERO_PARAMS)
@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_read_from(buffer_size, zero, hole, progress):
    src_extents = create_zero_extents("B0-")
    src_backing = create_backing("B0-")

    dst_backing = create_backing(
        "AAA" if zero and hole else "AA0" if zero else "A00")

    src = memory.Backend(
        mode="r", data=src_backing, extents={"zero": src_extents})

    dst = memory.ReaderFrom("r+", data=dst_backing)

    io.copy(
        src, dst,
        max_workers=1,
        buffer_size=buffer_size,
        zero=zero,
        hole=hole,
        progress=progress)

    assert dst_backing == src_backing


@pytest.mark.parametrize("buffer_size", [128, 1024])
@pytest.mark.parametrize("zero,hole", ZERO_PARAMS)
@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_write_to(buffer_size, zero, hole, progress):
    src_extents = create_zero_extents("B0-")
    src_backing = create_backing("B0-")

    dst_backing = create_backing(
        "AAA" if zero and hole else "AA0" if zero else "A00")

    src = memory.WriterTo(
        mode="r", data=src_backing, extents={"zero": src_extents})

    dst = memory.Backend("r+", data=dst_backing)

    io.copy(
        src, dst,
        max_workers=1,
        buffer_size=buffer_size,
        zero=zero,
        hole=hole,
        progress=progress)

    assert dst_backing == src_backing


@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_dirty(progress):
    src = memory.Backend(
        mode="r",
        data=create_backing("ABCD"),
        extents={"dirty": create_dirty_extents("AbCd")},
    )
    dst_backing = create_backing("0000")
    dst = memory.Backend("r+", data=dst_backing)

    io.copy(src, dst, dirty=True, max_workers=1, progress=progress)

    # Copy dirty extents, skip clean extents.
    assert dst_backing == create_backing("A0C0")


@pytest.mark.parametrize("zero,hole", ZERO_PARAMS)
def test_copy_data_progress(zero, hole):
    src = memory.Backend(
        mode="r",
        data=create_backing("A0C-"),
        extents={"zero": create_zero_extents("A0C-")},
    )
    dst_backing = create_backing("0000")
    dst = memory.Backend("r+", data=dst_backing)

    p = FakeProgress()
    io.copy(src, dst, max_workers=1, zero=zero, hole=hole, progress=p)

    # Report at least every extent.
    assert len(p.updates) >= 4

    # Report entire image size.
    assert sum(p.updates) == len(dst_backing)


def test_copy_dirty_progress():
    src = memory.Backend(
        mode="r",
        data=create_backing("A0C-"),
        extents={"dirty": create_dirty_extents("A0C-")},
    )
    dst_backing = create_backing("0000")
    dst = memory.Backend("r+", data=dst_backing)

    p = FakeProgress()
    io.copy(src, dst, dirty=True, max_workers=1, progress=p)

    # Report at least every extent.
    assert len(p.updates) >= 4

    # Report entire image size.
    assert sum(p.updates) == len(dst_backing)


class BackendError(Exception):
    pass


class FailingBackend:

    def __init__(self, fail_read=False, fail_write=False, delay=0.1):
        self.fail_read = fail_read
        self.fail_write = fail_write
        self.delay = delay

    def clone(self):
        return FailingBackend(
            fail_read=self.fail_read, fail_write=self.fail_write)

    def size(self):
        return 10 * 1024**3

    def extents(self, ctx="zero"):
        return [extent.ZeroExtent(0, self.size(), False, False)]

    def readinto(self, buf):
        time.sleep(self.delay)
        if self.fail_read:
            raise BackendError("read error")
        return len(buf)

    def write(self, buf):
        time.sleep(self.delay)
        if self.fail_write:
            raise BackendError("write error")
        return len(buf)

    def seek(self, n, how=None):
        pass

    def close(self):
        pass


def test_reraise_dst_error():
    src = FailingBackend()
    dst = FailingBackend(fail_write=True)
    with pytest.raises(BackendError) as e:
        io.copy(src, dst)
    assert str(e.value) == "write error"


def test_reraise_src_error():
    src = FailingBackend(fail_read=True)
    dst = FailingBackend()
    with pytest.raises(BackendError) as e:
        io.copy(src, dst)
    assert str(e.value) == "read error"


# Testing helpers.

def populate_image(path, fmt, extents):
    with qemu_nbd.open(path, fmt) as c:
        offset = 0
        for kind, length in extents:
            if kind == "data":
                c.write(offset, b"x" * length)
            elif kind == "zero":
                c.zero(offset, length, punch_hole=False)
            elif kind == "hole":
                pass  # Unallocated
            offset += length
        c.flush()


def create_zero_extents(fmt):
    """
    Create zero extents from format string.

    "A0-" -> [
        ZeroExtent(0 * CHUNK_SIZE, CHUNK_SIZE, False, False),
        ZeroExtent(1 * CHUNK_SIZE, CHUNK_SIZE, True, False),
        ZeroExtent(2 * CHUNK_SIZE, CHUNK_SIZE, True, True),
    ]
    """
    extents = []
    offset = 0

    for c in fmt:
        if c == "0":
            extents.append(
                extent.ZeroExtent(offset, CHUNK_SIZE, zero=True, hole=False))
        elif c == "-":
            extents.append(
                extent.ZeroExtent(offset, CHUNK_SIZE, zero=True, hole=True))
        else:
            extents.append(
                extent.ZeroExtent(offset, CHUNK_SIZE, zero=False, hole=False))
        offset += CHUNK_SIZE

    return extents


def create_dirty_extents(fmt):
    """
    Create dirty extents from format string.

    "Ab" -> [
        DirtyExtent(0 * CHUNK_SIZE, CHUNK_SIZE, True, False),
        DirtyExtent(1 * CHUNK_SIZE, CHUNK_SIZE, False, False),
    ]
    """
    extents = []
    offset = 0

    for c in fmt:
        extents.append(
            extent.DirtyExtent(
                offset, CHUNK_SIZE, dirty=c.isupper(), zero=False))
        offset += CHUNK_SIZE

    return extents


def create_backing(fmt):
    """
    Create backing from format string.

    "A0-" -> bytearray(
        b"A" * CHUNK_SIZE + b"\0" * CHUNK_SIZE + b"\0" * CHUNK_SIZE
    )
    """
    b = bytearray()

    for c in fmt:
        if c in ("0", "-"):
            b += b"\0" * CHUNK_SIZE
        else:
            b += c.encode("ascii") * CHUNK_SIZE

    return b
