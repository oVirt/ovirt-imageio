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

from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import io
from ovirt_imageio._internal.backends import nbd, memory, image
from ovirt_imageio._internal.nbd import UnixAddress

ZERO_PARAMS = [
    pytest.param(True, id="zero"),
    pytest.param(False, id="nozero"),
]


class FakeProgress:

    def __init__(self):
        self.updates = []

    def update(self, n):
        self.updates.append(n)


@pytest.mark.parametrize("src_fmt,dst_fmt", [
    ("raw", "raw"),
    ("qcow2", "qcow2"),
    ("raw", "qcow2"),
    ("qcow2", "raw"),
])
@pytest.mark.parametrize("zero", ZERO_PARAMS)
def test_copy_nbd_to_nbd(tmpdir, src_fmt, dst_fmt, zero):
    # Make sure we have zero extents larger than MAX_ZERO_SIZE (1 GiB). It
    # would be nice to have also data extents larger than MAX_COPY_SIZE (128
    # MiB), but this is too slow for automated tests.
    size = 2 * io.MAX_ZERO_SIZE

    # Default cluser size with qcow2 format.
    cluster_size = 64 * 1024

    src = str(tmpdir.join("src." + src_fmt))
    qemu_img.create(src, src_fmt, size=size)

    with qemu_nbd.open(src, src_fmt) as c:
        # Create first data extent.
        c.write(0, b"data extent 1\n")

        # Between the data extents we have a zero extent bigger than
        # io.MAX_ZERO_SIZE.

        # Create data extent larger than io.BUFFER_SIZE.
        data = b"data extent 2\n" + b"x" * io.BUFFER_SIZE
        c.write(io.MAX_ZERO_SIZE + 2 * cluster_size, data)

        # Between the data extents we have a zero extent smaller than
        # io.MAX_ZERO_SIZE.

        # Create last data extent at the end of the image.
        c.write(size - 4096, b"data extent 3\n")

        c.flush()

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
            nbd.open(dst_url, "r+") as dst_backend:

        # Because we copy to new image, we can alays use zero=False, but we
        # test both to verify that the result is the same.
        io.copy(src_backend, dst_backend, max_workers=max_workers, zero=zero)

    qemu_img.compare(src, dst)


@pytest.mark.parametrize("buffer_size", [128, 1024])
@pytest.mark.parametrize("zero", ZERO_PARAMS)
@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_generic(buffer_size, zero, progress):
    size = 1024
    chunk_size = size // 2

    src_backing = bytearray(b"x" * chunk_size + b"\0" * chunk_size)
    dst_backing = bytearray((b"y" if zero else b"\0") * size)

    src = memory.Backend(
        mode="r",
        data=src_backing,
        extents={
            "zero": [
                image.ZeroExtent(0 * chunk_size, chunk_size, False, False),
                image.ZeroExtent(1 * chunk_size, chunk_size, True, False),
            ]
        }
    )

    dst = memory.Backend("r+", data=dst_backing)

    io.copy(
        src, dst,
        max_workers=1,
        buffer_size=buffer_size,
        zero=zero,
        progress=progress)

    assert dst_backing == src_backing


@pytest.mark.parametrize("buffer_size", [128, 1024])
@pytest.mark.parametrize("zero", ZERO_PARAMS)
@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_read_from(buffer_size, zero, progress):
    size = 1024
    chunk_size = size // 2

    src_backing = bytearray(b"x" * chunk_size + b"\0" * chunk_size)
    dst_backing = bytearray((b"y" if zero else b"\0") * size)

    src = memory.Backend(
        mode="r",
        data=src_backing,
        extents={
            "zero": [
                image.ZeroExtent(0 * chunk_size, chunk_size, False, False),
                image.ZeroExtent(1 * chunk_size, chunk_size, True, False),
            ]
        }
    )

    dst = memory.ReaderFrom("r+", data=dst_backing)

    io.copy(
        src, dst,
        max_workers=1,
        buffer_size=buffer_size,
        zero=zero,
        progress=progress)

    assert dst_backing == src_backing


@pytest.mark.parametrize("buffer_size", [128, 1024])
@pytest.mark.parametrize("zero", ZERO_PARAMS)
@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_write_to(buffer_size, zero, progress):
    size = 1024
    chunk_size = size // 2

    src_backing = bytearray(b"x" * chunk_size + b"\0" * chunk_size)
    dst_backing = bytearray((b"y" if zero else b"\0") * size)

    src = memory.WriterTo(
        mode="r",
        data=src_backing,
        extents={
            "zero": [
                image.ZeroExtent(0 * chunk_size, chunk_size, False, False),
                image.ZeroExtent(1 * chunk_size, chunk_size, True, False),
            ]
        }
    )

    dst = memory.Backend("r+", data=dst_backing)

    io.copy(
        src, dst,
        max_workers=1,
        buffer_size=buffer_size,
        zero=zero,
        progress=progress)

    assert dst_backing == src_backing


@pytest.mark.parametrize("progress", [None, FakeProgress()])
def test_copy_dirty(progress):
    size = 1024
    chunk_size = size // 4

    src = memory.Backend(
        mode="r",
        data=bytearray(
            b"a" * chunk_size +
            b"b" * chunk_size +
            b"c" * chunk_size +
            b"d" * chunk_size
        ),
        extents={
            "dirty": [
                image.DirtyExtent(0 * chunk_size, chunk_size, True),
                image.DirtyExtent(1 * chunk_size, chunk_size, False),
                image.DirtyExtent(2 * chunk_size, chunk_size, True),
                image.DirtyExtent(3 * chunk_size, chunk_size, False),
            ]
        }
    )

    dst_backing = bytearray(b"\0" * size)
    dst = memory.Backend("r+", data=dst_backing)

    io.copy(src, dst, dirty=True, max_workers=1, progress=progress)

    assert dst_backing == (
        b"a" * chunk_size +
        b"\0" * chunk_size +
        b"c" * chunk_size +
        b"\0" * chunk_size
    )


@pytest.mark.parametrize("zero", ZERO_PARAMS)
def test_copy_data_progress(zero):
    size = 1024
    chunk_size = size // 4

    src = memory.Backend(
        mode="r",
        data=bytearray(
            b"x" * chunk_size +
            b"\0" * chunk_size +
            b"x" * chunk_size +
            b"\0" * chunk_size
        ),
        extents={
            "zero": [
                image.ZeroExtent(0 * chunk_size, chunk_size, False, False),
                image.ZeroExtent(1 * chunk_size, chunk_size, True, False),
                image.ZeroExtent(2 * chunk_size, chunk_size, False, False),
                image.ZeroExtent(3 * chunk_size, chunk_size, True, False),
            ]
        }
    )

    dst = memory.Backend("r+", data=bytearray(b"\0" * size))

    p = FakeProgress()
    io.copy(src, dst, max_workers=1, zero=zero, progress=p)

    # Report at least every extent.
    assert len(p.updates) >= 4

    # Report entire image size.
    assert sum(p.updates) == size


def test_copy_dirty_progress():
    size = 1024
    chunk_size = size // 4

    src = memory.Backend(
        mode="r",
        data=bytearray(
            b"x" * chunk_size +
            b"\0" * chunk_size +
            b"x" * chunk_size +
            b"\0" * chunk_size
        ),
        extents={
            "dirty": [
                image.DirtyExtent(0 * chunk_size, chunk_size, True),
                image.DirtyExtent(1 * chunk_size, chunk_size, False),
                image.DirtyExtent(2 * chunk_size, chunk_size, True),
                image.DirtyExtent(3 * chunk_size, chunk_size, False),
            ]
        }
    )

    dst = memory.Backend("r+", bytearray(b"\0" * size))

    p = FakeProgress()
    io.copy(src, dst, dirty=True, max_workers=1, progress=p)

    # Report at least every extent.
    assert len(p.updates) >= 4

    # Report entire image size.
    assert sum(p.updates) == size


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
        return [image.ZeroExtent(0, self.size(), False, False)]

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
