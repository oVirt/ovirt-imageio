# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import subprocess

import pytest

from ovirt_imageio_common import nbd

from . import qemu_nbd

log = logging.getLogger("test")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
@pytest.mark.parametrize("export", [
    "",
    "ascii",
    pytest.param(u"\u05d0", id="unicode"),
])
def test_handshake(tmpdir, export, fmt):
    image = str(tmpdir.join("image"))
    subprocess.check_call(["qemu-img", "create", "-f", fmt, image, "1g"])
    sock = str(tmpdir.join("sock"))

    with qemu_nbd.run(image, fmt, sock, export_name=export):
        if export:
            c = nbd.Client(sock, export)
        else:
            c = nbd.Client(sock)
        with c:
            # TODO: test transmission_flags?
            assert c.export_size == 1024**3
            assert c.minimum_block_size == 1
            assert c.preferred_block_size == 4096
            assert c.maximum_block_size == 32 * 1024**2


def test_raw_read(tmpdir):
    image = str(tmpdir.join("image"))
    sock = str(tmpdir.join("sock"))
    offset = 1024**2
    data = b"can read from raw"

    with open(image, "wb") as f:
        f.truncate(1024**3)
        f.seek(offset)
        f.write(data)

    with qemu_nbd.run(image, "raw", sock):
        with nbd.Client(sock) as c:
            assert c.read(offset, len(data)) == data


def test_raw_write(tmpdir):
    image = str(tmpdir.join("image"))
    with open(image, "wb") as f:
        f.truncate(1024**3)
    sock = str(tmpdir.join("sock"))
    offset = 1024**2
    data = b"can write to raw"

    with qemu_nbd.run(image, "raw", sock):
        with nbd.Client(sock) as c:
            c.write(offset, data)
            c.flush()

    with open(image, "rb") as f:
        f.seek(offset)
        assert f.read(len(data)) == data


def test_qcow2_write_read(tmpdir):
    image = str(tmpdir.join("image"))
    sock = str(tmpdir.join("sock"))
    offset = 1024**2
    data = b"can read and write qcow2"
    subprocess.check_call(["qemu-img", "create", "-f", "qcow2", image, "1g"])

    with qemu_nbd.run(image, "qcow2", sock):
        with nbd.Client(sock) as c:
            c.write(offset, data)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, len(data)) == data


@pytest.mark.parametrize("format", ["raw", "qcow2"])
def test_zero(tmpdir, format):
    size = 2 * 1024**2
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = str(tmpdir.join("sock"))
    subprocess.check_call(
        ["qemu-img", "create", "-f", format, image, str(size)])

    with qemu_nbd.run(image, format, sock):
        # Fill image with data
        with nbd.Client(sock) as c:
            c.write(0, b"x" * size)
            c.flush()

        # Zero a range
        with nbd.Client(sock) as c:
            c.zero(offset, 4096)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, 4096) == b"\0" * 4096


@pytest.mark.parametrize("format", ["raw", "qcow2"])
def test_zero_max_block_size(tmpdir, format):
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = str(tmpdir.join("sock"))
    subprocess.check_call(
        ["qemu-img", "create", "-f", format, image, "1g"])

    with qemu_nbd.run(image, format, sock):
        # Fill range with data
        with nbd.Client(sock) as c:
            size = c.maximum_block_size
            c.write(offset, b"x" * size)
            c.flush()

        # Zero range using maximum block size
        with nbd.Client(sock) as c:
            c.zero(offset, size)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, size) == b"\0" * size


@pytest.mark.parametrize("format", ["raw", "qcow2"])
def test_zero_min_block_size(tmpdir, format):
    offset = 1024**2
    image = str(tmpdir.join("image"))
    sock = str(tmpdir.join("sock"))
    subprocess.check_call(
        ["qemu-img", "create", "-f", format, image, "1g"])

    with qemu_nbd.run(image, format, sock):
        # Fill range with data
        with nbd.Client(sock) as c:
            size = c.minimum_block_size
            c.write(offset, b"x" * size)
            c.flush()

        # Zero range using minimum block size
        with nbd.Client(sock) as c:
            c.zero(offset, size)
            c.flush()

        with nbd.Client(sock) as c:
            assert c.read(offset, size) == b"\0" * size
