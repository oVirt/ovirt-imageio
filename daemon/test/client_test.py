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
from ovirt_imageio._internal import ipv6
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import server

from . import testutil

from . marks import requires_python3

pytestmark = requires_python3

CLUSTER_SIZE = 64 * 1024
IMAGE_SIZE = 3 * CLUSTER_SIZE


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

    host, port = srv.remote_service.address
    host = ipv6.quote_address(host)
    return "https://{}:{}/images/{}".format(host, port, ticket["uuid"])


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


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_empty_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    # TODO: Check why allocation differ when src is qcow2. Target image
    # allocation is 0 bytes as expected, but comparing with strict=True fail at
    # offset 0.
    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_start_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(IMAGE_SIZE - 6, b"middle")
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_middle_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"start")
        c.write(IMAGE_SIZE - 3, b"end")
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_end_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"start")
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_full_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"b" * IMAGE_SIZE)
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_preallocated(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

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


def test_download_qcow2_as_raw(tmpdir, srv):
    src = str(tmpdir.join("src.qcow2"))
    qemu_img.create(src, "qcow2", size=IMAGE_SIZE)

    # Allocate one cluster in the middle of the image.
    with qemu_nbd.open(src, "qcow2") as c:
        c.write(CLUSTER_SIZE, b"a" * CLUSTER_SIZE)
        c.flush()

    actual_size = os.path.getsize(src)
    url = prepare_transfer(srv, src, size=actual_size)
    dst = str(tmpdir.join("dst.qcow2"))

    # When downloading qcow2 image using the nbd backend, we get raw data and
    # we can convert it to any format we want. Howver when downloading using
    # the file backend, we get qcow2 bytestream and we cannot convert it.
    #
    # To store the qcow2 bytestream, we must use fmt="raw". This instructs
    # qemu-nbd on the client side to treat the data as raw bytes, storing them
    # without any change on the local file.
    #
    # This is baisically like:
    #
    #   qemu-img convert -f raw -O raw src.qcow2 dst.qcow2
    #
    client.download(url, dst, srv.config.tls.ca_file, fmt="raw")

    # The result should be identical qcow2 image content. Allocation may
    # differ but for this test we get identical allocation.
    qemu_img.compare(src, dst, format1="qcow2", format2="qcow2", strict=True)


def test_upload_proxy_url(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    # If transfer_url is not accessible, proxy_url is used.
    transfer_url = "https://no.server:54322/images/no-ticket"
    proxy_url = prepare_transfer(srv, dst)

    client.upload(src, transfer_url, srv.config.tls.ca_file,
                  proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw", strict=True)


def test_upload_proxy_url_unused(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    # If transfer_url is accessible, proxy_url is not used.
    transfer_url = prepare_transfer(srv, dst)
    proxy_url = "https://no.proxy:54322/images/no-ticket"

    client.upload(src, transfer_url, srv.config.tls.ca_file,
                  proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw", strict=True)


def test_download_proxy_url(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))

    # If transfer_url is not accessible, proxy_url is used.
    transfer_url = "https://no.server:54322/images/no-ticket"
    proxy_url = prepare_transfer(srv, src)

    client.download(transfer_url, dst, srv.config.tls.ca_file, fmt="raw",
                    proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw")


def test_download_proxy_url_unused(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))

    # If transfer_url is accessible, proxy_url is not used.
    transfer_url = prepare_transfer(srv, src)
    proxy_url = "https://no.proxy:54322/images/no-ticket"

    client.download(transfer_url, dst, srv.config.tls.ca_file, fmt="raw",
                    proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw")


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

    # Note: when using multiple connections order of updates is not
    # predictable.
    assert set(progress.updates) == {
        # First write.
        4096,
        # First zero.
        IMAGE_SIZE // 2 - 4096,
        # Second write.
        4096,
        # Second zero
        IMAGE_SIZE // 2 - 4096,
    }


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
