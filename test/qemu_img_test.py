# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd


@pytest.mark.parametrize("src_fmt,dst_fmt", [
    ("raw", "raw"),
    ("qcow2", "qcow2"),
    ("raw", "qcow2"),
])
def test_compare_identical_content(tmpdir, src_fmt, dst_fmt):
    size = 1024**2
    src = str(tmpdir.join("src." + src_fmt))
    dst = str(tmpdir.join("dst." + dst_fmt))

    qemu_img.create(src, src_fmt, size=size)
    qemu_img.create(dst, dst_fmt, size=size)

    # Destination image has different allocation.
    with qemu_nbd.open(dst, dst_fmt) as c:
        c.write(size // 2, b"\0")
        c.flush()

    qemu_img.compare(src, dst, format1=src_fmt, format2=dst_fmt)


@pytest.mark.parametrize("src_fmt,dst_fmt", [
    ("raw", "raw"),
    ("qcow2", "qcow2"),
    ("raw", "qcow2"),
])
def test_compare_different_content(tmpdir, src_fmt, dst_fmt):
    size = 1024**2
    src = str(tmpdir.join("src." + src_fmt))
    dst = str(tmpdir.join("dst." + dst_fmt))

    qemu_img.create(src, src_fmt, size=size)
    qemu_img.create(dst, dst_fmt, size=size)

    # Destination image has different content.
    with qemu_nbd.open(dst, dst_fmt) as c:
        c.write(size // 2, b"x")
        c.flush()

    with pytest.raises(qemu_img.ContentMismatch):
        qemu_img.compare(src, dst, format1=src_fmt, format2=dst_fmt)


def test_compare_wrong_format(tmpdir):
    size = 1024**2
    src = str(tmpdir.join("src.raw"))
    dst = str(tmpdir.join("dst.raw"))

    qemu_img.create(src, "raw", size=size)
    qemu_img.create(dst, "raw", size=size)

    with pytest.raises(qemu_img.OpenImageError):
        qemu_img.compare(src, dst, format1="qcow2")

    with pytest.raises(qemu_img.OpenImageError):
        qemu_img.compare(src, dst, format2="qcow2")


@pytest.mark.parametrize("src_fmt,dst_fmt", [
    ("raw", "raw"),
    ("qcow2", "qcow2"),
    ("raw", "qcow2"),
])
def test_compare_different_allocation(tmpdir, src_fmt, dst_fmt):
    # Images has same content, but different allocation.
    size = 1024**2
    src = str(tmpdir.join("src." + src_fmt))
    dst = str(tmpdir.join("dst." + dst_fmt))

    qemu_img.create(src, src_fmt, size=size)
    qemu_img.create(dst, dst_fmt, size=size)

    with qemu_nbd.open(dst, dst_fmt) as c:
        c.write(size // 2, b"\0")
        c.flush()

    with pytest.raises(qemu_img.ContentMismatch):
        qemu_img.compare(
            src, dst, format1=src_fmt, format2=dst_fmt, strict=True)


def test_compare_missing_file(tmpdir):
    src = str(tmpdir.join("src.raw"))
    dst = str(tmpdir.join("dst.raw"))

    qemu_img.create(src, "raw", size=1024**2)

    with pytest.raises(qemu_img.OpenImageError):
        qemu_img.compare(src, dst)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_create_info(tmpdir, fmt):
    size = 1024**2
    image = str(tmpdir.join("image." + fmt))
    qemu_img.create(image, fmt, size=size)
    info = qemu_img.info(image)

    assert info["filename"] == image
    assert info["virtual-size"] == size
    assert info["format"] == fmt


def test_add_bitmap(tmpdir):
    size = 10 * 1024**2
    img = str(tmpdir.join("img.qcow2"))
    qemu_img.create(img, "qcow2", size=size)
    qemu_img.bitmap_add(img, "b0")
    bitmaps = qemu_img.info(img)["format-specific"]["data"]["bitmaps"]
    assert bitmaps == [
        {"name": "b0", "flags": ["auto"], "granularity": 65536}
    ]
