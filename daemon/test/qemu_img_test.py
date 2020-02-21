# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest

from ovirt_imageio_common import qemu_img
from ovirt_imageio_common import qemu_nbd

from . marks import requires_python3


@pytest.mark.parametrize("src_fmt,dst_fmt", [
    ("raw", "raw"),
    ("qcow2", "qcow2"),
    ("raw", "qcow2"),
])
def test_compare_identical(tmpdir, src_fmt, dst_fmt):
    size = 1024**2
    src = str(tmpdir.join("src." + src_fmt))
    dst = str(tmpdir.join("dst." + dst_fmt))

    qemu_img.create(src, src_fmt, size=size)
    qemu_img.create(dst, dst_fmt, size=size)

    qemu_img.compare(src, dst)


@requires_python3
@pytest.mark.parametrize("src_fmt,dst_fmt", [
    ("raw", "raw"),
    ("qcow2", "qcow2"),
    ("raw", "qcow2"),
])
def test_compare_different(tmpdir, src_fmt, dst_fmt):
    size = 1024**2
    src = str(tmpdir.join("src." + src_fmt))
    dst = str(tmpdir.join("dst." + dst_fmt))

    qemu_img.create(src, src_fmt, size=size)
    qemu_img.create(dst, dst_fmt, size=size)

    with qemu_nbd.open(dst, dst_fmt) as c:
        c.write(size // 2, b"x")
        c.flush()

    with pytest.raises(qemu_img.ContentMismatch):
        qemu_img.compare(src, dst)


def test_compare_error(tmpdir):
    src = str(tmpdir.join("src.raw"))
    dst = str(tmpdir.join("dst.raw"))

    qemu_img.create(src, "raw", size=1024**2)

    with pytest.raises(RuntimeError):
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
