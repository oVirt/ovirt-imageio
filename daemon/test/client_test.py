# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import os

import pytest

from ovirt_imageio import client
from ovirt_imageio._internal import config
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import server

from . import testutil

from . marks import requires_python3

pytestmark = requires_python3

IMAGE_SIZE = 128 * 1024


@pytest.fixture(scope="module")
def srv():
    cfg = config.load(["test/conf/daemon.conf"])
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


def prepare_transfer(srv, dst, sparse=True, size=IMAGE_SIZE):
    ticket = testutil.create_ticket(
        url="file://" + dst,
        size=size,
        sparse=sparse,
        ops=["read", "write"])

    srv.auth.add(ticket)

    return "https://127.0.0.1:{}/images/{}".format(
        srv.remote_service.port, ticket["uuid"])


class FakeProgress:

    def __init__(self):
        self.size = None
        self.updates = []

    def update(self, n):
        self.updates.append(n)


# TODO:
# - verify that upload optimized the upload using unix socket. Need a way to
#   enable only OPTIONS on the remote server.
# - verify that upload fall back to HTTPS if server does not support unix
#   socket. We don't have a way to disable unix socket currently.
# - verify that upload fall back to HTTPS if server support unix socket but is
#   not the local host. Probbly not feasble for these tests, unless we can
#   start a daemon on another host.
# - Test negative flows


def test_upload_empty_sparse(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


def test_upload_hole_at_start_sparse(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"b" * (IMAGE_SIZE // 2))

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


def test_upload_hole_at_middle_sparse(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.write(b"b" * (IMAGE_SIZE // 4))
        f.seek(IMAGE_SIZE // 2, os.SEEK_CUR)
        f.write(b"b" * (IMAGE_SIZE // 4))

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


def test_upload_hole_at_end_sparse(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.write(b"b" * (IMAGE_SIZE // 2))

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


def test_upload_full_sparse(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.write(b"b" * IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


def test_upload_preallocated(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst, sparse=False)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst)
    assert os.stat(dst).st_blocks * 512 == IMAGE_SIZE


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_download_raw(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"data")

    url = prepare_transfer(srv, src)
    dst = str(tmpdir.join("dst"))

    # When we download raw data, we can convert it on-the-fly to other format.
    client.download(url, dst, srv.config.tls.ca_file, fmt=fmt)

    # file backend does not support extents, so downloaded data is always
    # fully allocated.
    qemu_img.compare(src, dst, format1="raw", format2=fmt)


def test_progress(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.write(b"b" * 4096)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"b" * 4096)
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    url = prepare_transfer(srv, dst, sparse=True)

    progress = FakeProgress()
    client.upload(
        src, url, srv.config.tls.ca_file, progress=progress)

    assert progress.size == IMAGE_SIZE
    assert progress.updates == [
        # First write.
        4096,
        # First zero.
        IMAGE_SIZE // 2 - 4096,
        # Second write.
        4096,
        # Second zero
        IMAGE_SIZE // 2 - 4096,
    ]


def test_progress_callback(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    url = prepare_transfer(srv, dst, size=IMAGE_SIZE, sparse=True)

    progress = []
    client.upload(
        src,
        url,
        srv.config.tls.ca_file,
        progress=progress.append)

    assert progress == [IMAGE_SIZE]
