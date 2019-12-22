# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest

from six.moves.urllib_parse import urlparse

from ovirt_imageio_common import qemu_img
from ovirt_imageio_common import qemu_nbd
from ovirt_imageio_common import io
from ovirt_imageio_common.backends import nbd, memory, image
from ovirt_imageio_common.nbd import UnixAddress

from . marks import requires_python3

pytestmark = requires_python3

ZERO_PARAMS = [
    pytest.param(True, id="zero"),
    pytest.param(False, id="nozero"),
]


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

    with qemu_nbd.run(src, src_fmt, src_sock, read_only=True), \
            qemu_nbd.run(dst, dst_fmt, dst_sock), \
            nbd.open(src_url, "r") as src_backend, \
            nbd.open(dst_url, "r+") as dst_backend:

        # Because we copy to new image, we can alays use zero=False, but we
        # test both to verify that the result is the same.
        io.copy(src_backend, dst_backend, zero=zero)

    qemu_img.compare(src, dst)


@pytest.mark.parametrize("zero", ZERO_PARAMS)
def test_copy_generic(zero):
    size = 1024
    chunk_size = size // 2

    def fake_extents(context="zero"):
        return [
            image.ZeroExtent(0 * chunk_size, chunk_size, False),
            image.ZeroExtent(1 * chunk_size, chunk_size, True),
        ]

    src = memory.Backend("r", b"x" * chunk_size + b"\0" * chunk_size)
    src.extents = fake_extents

    dst = memory.Backend("r+", (b"y" if zero else b"\0") * size)

    io.copy(src, dst, buffer_size=128, zero=zero)

    assert dst.size() == src.size()
    assert dst.data() == src.data()


@pytest.mark.parametrize("zero", ZERO_PARAMS)
def test_copy_read_from(zero):
    size = 1024
    chunk_size = size // 2

    def fake_extents(context="zero"):
        return [
            image.ZeroExtent(0 * chunk_size, chunk_size, False),
            image.ZeroExtent(1 * chunk_size, chunk_size, True),
        ]

    src = memory.Backend("r", b"x" * chunk_size + b"\0" * chunk_size)
    src.extents = fake_extents

    dst = memory.ReaderFrom("r+", (b"y" if zero else b"\0") * size)

    io.copy(src, dst, buffer_size=128)

    assert dst.size() == src.size()
    assert dst.data() == src.data()


@pytest.mark.parametrize("zero", ZERO_PARAMS)
def test_copy_write_to(zero):
    size = 1024
    chunk_size = size // 2

    def fake_extents(context="zero"):
        return [
            image.ZeroExtent(0 * chunk_size, chunk_size, False),
            image.ZeroExtent(1 * chunk_size, chunk_size, True),
        ]

    src = memory.WriterTo("r", b"x" * chunk_size + b"\0" * chunk_size)
    src.extents = fake_extents

    dst = memory.Backend("r+", (b"y" if zero else b"\0") * size)

    io.copy(src, dst, buffer_size=128, zero=zero)

    assert dst.size() == src.size()
    assert dst.data() == src.data()


def test_copy_dirty():
    size = 1024
    chunk_size = size // 4

    def fake_extents(context="zero"):
        return [
            image.DirtyExtent(0 * chunk_size, chunk_size, True),
            image.DirtyExtent(1 * chunk_size, chunk_size, False),
            image.DirtyExtent(2 * chunk_size, chunk_size, True),
            image.DirtyExtent(3 * chunk_size, chunk_size, False),
        ]

    src = memory.Backend("r", (
        b"a" * chunk_size +
        b"b" * chunk_size +
        b"c" * chunk_size +
        b"d" * chunk_size
    ))
    src.extents = fake_extents

    dst = memory.Backend("r+", b"\0" * size)

    io.copy(src, dst, dirty=True)

    assert dst.data() == (
        b"a" * chunk_size +
        b"\0" * chunk_size +
        b"c" * chunk_size +
        b"\0" * chunk_size
    )


class FakeProgress:

    def __init__(self):
        self.updates = []

    def update(self, n):
        self.updates.append(n)


@pytest.mark.parametrize("zero", ZERO_PARAMS)
def test_copy_data_progress(zero):
    size = 1024
    chunk_size = size // 4

    def fake_extents(context="zero"):
        return [
            image.ZeroExtent(0 * chunk_size, chunk_size, False),
            image.ZeroExtent(1 * chunk_size, chunk_size, True),
            image.ZeroExtent(2 * chunk_size, chunk_size, False),
            image.ZeroExtent(3 * chunk_size, chunk_size, True),
        ]

    src = memory.Backend("r", (
        b"x" * chunk_size +
        b"\0" * chunk_size +
        b"x" * chunk_size +
        b"\0" * chunk_size
    ))
    src.extents = fake_extents

    dst = memory.Backend("r+", b"\0" * size)

    p = FakeProgress()
    io.copy(src, dst, zero=zero, progress=p)

    # Report at least every extent.
    assert len(p.updates) >= 4

    # Report entire image size.
    assert sum(p.updates) == size


def test_copy_dirty_progress():
    size = 1024
    chunk_size = size // 4

    def fake_extents(context="zero"):
        return [
            image.DirtyExtent(0 * chunk_size, chunk_size, True),
            image.DirtyExtent(1 * chunk_size, chunk_size, False),
            image.DirtyExtent(2 * chunk_size, chunk_size, True),
            image.DirtyExtent(3 * chunk_size, chunk_size, False),
        ]

    src = memory.Backend("r", (
        b"x" * chunk_size +
        b"\0" * chunk_size +
        b"x" * chunk_size +
        b"\0" * chunk_size
    ))
    src.extents = fake_extents

    dst = memory.Backend("r+", b"\0" * size)

    p = FakeProgress()
    io.copy(src, dst, dirty=True, progress=p)

    # Report at least every extent.
    assert len(p.updates) >= 4

    # Report entire image size.
    assert sum(p.updates) == size
