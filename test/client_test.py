# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import os
import tarfile
import logging

from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from ovirt_imageio import client
from ovirt_imageio._internal import blkhash
from ovirt_imageio._internal import config
from ovirt_imageio._internal import ipv6
from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import nbdutil
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import server

from ovirt_imageio._internal.extent import ZeroExtent, DirtyExtent

from . import testutil
from . import ci

log = logging.getLogger("test")

CLUSTER_SIZE = 64 * 1024
IMAGE_SIZE = 3 * CLUSTER_SIZE


@pytest.fixture(scope="module")
def srv():
    cfg = config.load(["test/conf/daemon.conf"])
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


def prepare_transfer(srv, url, sparse=True, size=IMAGE_SIZE):
    ticket = testutil.create_ticket(
        url=url,
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

    url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, url, srv.config.tls.ca_file)

    # TODO: Check why allocation differ when src is qcow2. Target image
    # allocation is 0 bytes as expected, but comparing with strict=True fail at
    # offset 0.
    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", [
    pytest.param(
        "raw",
        marks=pytest.mark.xfail(
            ci.is_ovirt(),
            reason="Broken in oVirt CI for unknown reason")
    ),
    "qcow2"
])
def test_upload_hole_at_start_sparse(tmpdir, srv, fmt):
    size = 3 * 1024**2
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=size)

    with qemu_nbd.open(src, fmt) as c:
        c.write(size - 1024**2, b"b" * 1024**2)
        c.flush()
        log.debug("src extents: %s", list(nbdutil.extents(c)))

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * size)

    url = prepare_transfer(srv, "file://" + dst, size=size)

    client.upload(src, url, srv.config.tls.ca_file)

    with qemu_nbd.open(dst, "raw", read_only=True) as c:
        log.debug("dst extents: %s", list(nbdutil.extents(c)))

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_middle_sparse(tmpdir, srv, fmt):
    size = 3 * 1024**2
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=size)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"b" * 1024**2)
        c.write(size - 1024**2, b"b" * 1024**2)
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * size)

    url = prepare_transfer(srv, "file://" + dst, size=size)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_end_sparse(tmpdir, srv, fmt):
    size = 3 * 1024**2
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=size)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"b" * 1024**2)
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * size)

    url = prepare_transfer(srv, "file://" + dst, size=size)

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

    url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_preallocated(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst, sparse=False)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst)
    assert os.stat(dst).st_blocks * 512 == IMAGE_SIZE


@pytest.mark.parametrize("fmt,compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_upload_from_ova(tmpdir, srv, fmt, compressed):
    offset = CLUSTER_SIZE
    data = b"I can eat glass and it doesn't hurt me."

    # Create raw disk with some data.
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.seek(offset)
        f.write(data)

    # Create source disk.
    src = str(tmpdir.join("src"))
    qemu_img.convert(tmp, src, "raw", fmt, compressed=compressed)

    # Create OVA package.
    ova = str(tmpdir.join("src.ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(src, arcname=os.path.basename(src))

    # Prepare destination file.
    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    # Test uploading src from ova.
    url = prepare_transfer(srv, "file://" + dst)
    client.upload(
        ova,
        url,
        srv.config.tls.ca_file,
        member=os.path.basename(src))

    qemu_img.compare(src, dst)


@pytest.mark.parametrize("base_fmt", ["raw", "qcow2"])
def test_upload_shallow(srv, nbd_server, tmpdir, base_fmt):
    size = 10 * 1024**2

    # Create base image with some data in first 3 clusters.
    src_base = str(tmpdir.join("src_base." + base_fmt))
    qemu_img.create(src_base, base_fmt, size=size)
    with qemu_nbd.open(src_base, base_fmt) as c:
        c.write(0 * CLUSTER_SIZE, b"a" * CLUSTER_SIZE)
        c.write(1 * CLUSTER_SIZE, b"b" * CLUSTER_SIZE)
        c.write(2 * CLUSTER_SIZE, b"c" * CLUSTER_SIZE)
        c.flush()
        log.debug("src_base extents: %s", list(nbdutil.extents(c)))

    # Create src image with some data in second cluster and zero in third
    # cluster.
    src_top = str(tmpdir.join("src_top.qcow2"))
    qemu_img.create(
        src_top, "qcow2", backing_file=src_base, backing_format=base_fmt)
    with qemu_nbd.open(src_top, "qcow2") as c:
        c.write(1 * CLUSTER_SIZE, b"B" * CLUSTER_SIZE)
        c.zero(2 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.flush()

    with qemu_nbd.open(
            src_top, "qcow2", read_only=True, backing_chain=False) as c:
        log.debug("src_top extents: %s", list(nbdutil.extents(c)))

    # Create empty destination base image.
    dst_base = str(tmpdir.join("dst_base." + base_fmt))
    qemu_img.create(dst_base, base_fmt, size=size)

    # Create empty destination top image.
    dst_top = str(tmpdir.join("dst_top.qcow2"))
    qemu_img.create(
        dst_top, "qcow2", backing_file=dst_base, backing_format=base_fmt)

    # Upload base image.

    nbd_server.image = dst_base
    nbd_server.fmt = base_fmt
    nbd_server.start()

    url = prepare_transfer(srv, nbd_server.sock.url(), size=size)
    client.upload(
        src_base,
        url,
        srv.config.tls.ca_file,
        backing_chain=False)

    nbd_server.stop()

    with qemu_nbd.open(dst_base, base_fmt, read_only=True) as c:
        log.debug("dst_base extents: %s", list(nbdutil.extents(c)))

    # Compare image content - must match.
    qemu_img.compare(
        src_base, dst_base, format1=base_fmt, format2=base_fmt, strict=False)

    # Comparing allocation is not possible with raw format since qemu-nbd does
    # not report allocation for raw images, so we treat unallocated areas as
    # zero area instead of a hole.
    if base_fmt == "qcow2":
        qemu_img.compare(
            src_base, dst_base, format1=base_fmt, format2=base_fmt,
            strict=True)

    # Upload top image.

    nbd_server.image = dst_top
    nbd_server.fmt = "qcow2"
    nbd_server.start()

    url = prepare_transfer(srv, nbd_server.sock.url(), size=size)
    client.upload(
        src_top,
        url,
        srv.config.tls.ca_file,
        backing_chain=False)

    nbd_server.stop()

    with qemu_nbd.open(
            dst_top, "qcow2", read_only=True, backing_chain=False) as c:
        log.debug("dst_top extents: %s", list(nbdutil.extents(c)))

    # Test image content - must match.
    qemu_img.compare(
        src_top, dst_top, format1="qcow2", format2="qcow2", strict=False)

    # Compare allocation for qcow2 chain - nice to have.
    if base_fmt == "qcow2":
        qemu_img.compare(
            src_top, dst_top, format1="qcow2", format2="qcow2", strict=True)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_download_raw(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"data")

    url = prepare_transfer(srv, "file://" + src)
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
    url = prepare_transfer(srv, "file://" + src, size=actual_size)
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


@pytest.mark.parametrize("base_fmt", ["raw", "qcow2"])
def test_download_shallow(srv, nbd_server, tmpdir, base_fmt):
    size = 10 * 1024**2

    # Create source base image with some data in first clusters.
    src_base = str(tmpdir.join("src_base." + base_fmt))
    qemu_img.create(src_base, base_fmt, size=size)
    with qemu_nbd.open(src_base, base_fmt) as c:
        c.write(0 * CLUSTER_SIZE, b"a" * CLUSTER_SIZE)
        c.write(1 * CLUSTER_SIZE, b"b" * CLUSTER_SIZE)
        c.write(2 * CLUSTER_SIZE, b"c" * CLUSTER_SIZE)
        c.flush()
        log.debug("src_base extents: %s", list(nbdutil.extents(c)))

    # Create source top image with some data in second cluster and zero in the
    # third cluster.
    src_top = str(tmpdir.join("src_top.qcow2"))
    qemu_img.create(
        src_top, "qcow2", backing_file=src_base, backing_format=base_fmt)
    with qemu_nbd.open(src_top, "qcow2") as c:
        c.write(1 * CLUSTER_SIZE, b"B" * CLUSTER_SIZE)
        c.zero(2 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.flush()

    with qemu_nbd.open(
            src_top, "qcow2", read_only=True, backing_chain=False) as c:
        log.debug("src_top extents: %s", list(nbdutil.extents(c)))

    dst_base = str(tmpdir.join("dst_base." + base_fmt))
    dst_top = str(tmpdir.join("dst_top.qcow2"))

    # Download base image.

    nbd_server.image = src_base
    nbd_server.fmt = base_fmt
    nbd_server.start()

    url = prepare_transfer(srv, nbd_server.sock.url(), size=size)
    client.download(
        url,
        dst_base,
        srv.config.tls.ca_file,
        fmt=base_fmt)

    nbd_server.stop()

    with qemu_nbd.open(dst_base, base_fmt, read_only=True) as c:
        log.debug("dst_base extents: %s", list(nbdutil.extents(c)))

    # Compare image content - must match.
    qemu_img.compare(
        src_base, dst_base, format1=base_fmt, format2=base_fmt, strict=False)

    # And allocation - nice to have.
    if base_fmt == "qcow2" or qemu_nbd.version() >= (6, 0, 0):
        qemu_img.compare(
            src_base, dst_base, format1=base_fmt, format2=base_fmt,
            strict=True)

    # Download top image.

    nbd_server.image = src_top
    nbd_server.fmt = "qcow2"
    nbd_server.backing_chain = False
    nbd_server.start()

    url = prepare_transfer(srv, nbd_server.sock.url(), size=size)
    client.download(
        url,
        dst_top,
        srv.config.tls.ca_file,
        backing_file=dst_base,
        backing_format=base_fmt)

    nbd_server.stop()

    with qemu_nbd.open(
            dst_top, "qcow2", read_only=True, backing_chain=False) as c:
        log.debug("dst_top extents: %s", list(nbdutil.extents(c)))

    # Compare both image content - must match.
    qemu_img.compare(
        src_top, dst_top, format1="qcow2", format2="qcow2", strict=False)

    # And allocation - nice to have.
    qemu_img.compare(
        src_top, dst_top, format1="qcow2", format2="qcow2", strict=True)


def test_upload_proxy_url(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    # If transfer_url is not accessible, proxy_url is used.
    transfer_url = "https://no.server:54322/images/no-ticket"
    proxy_url = prepare_transfer(srv, "file://" + dst)

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
    transfer_url = prepare_transfer(srv, "file://" + dst)
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
    proxy_url = prepare_transfer(srv, "file://" + src)

    client.download(transfer_url, dst, srv.config.tls.ca_file, fmt="raw",
                    proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw")


def test_download_proxy_url_unused(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))

    # If transfer_url is accessible, proxy_url is not used.
    transfer_url = prepare_transfer(srv, "file://" + src)
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

    url = prepare_transfer(srv, "file://" + dst, sparse=True)

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

    url = prepare_transfer(srv, "file://" + dst, size=IMAGE_SIZE, sparse=True)

    progress = []
    client.upload(
        src,
        url,
        srv.config.tls.ca_file,
        progress=progress.append)

    assert progress == [IMAGE_SIZE]


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_info(tmpdir, fmt, compressed):
    # Created temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)
    img_info = client.info(img)

    # Check image info.
    assert img_info["format"] == fmt
    assert img_info["virtual-size"] == size

    # We don't add member info if member was not specified.
    assert "member-offset" not in img_info
    assert "member-size" not in img_info

    # Create ova with test image.
    member = os.path.basename(img)
    ova = str(tmpdir.join("ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(img, arcname=member)

    # Get info for the member from the ova.
    ova_info = client.info(ova, member=member)

    # Image info from ova should be the same.
    assert ova_info["format"] == fmt
    assert ova_info["virtual-size"] == size

    # If member was specified, we report also the offset and size.
    with tarfile.open(ova) as tar:
        member_info = tar.getmember(member)
    assert ova_info["member-offset"] == member_info.offset_data
    assert ova_info["member-size"] == member_info.size


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_measure_to_raw(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    measure = client.measure(img, "raw")
    assert measure["required"] == size


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_measure_to_qcow2(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    measure = client.measure(img, "qcow2")
    assert measure["required"] == 393216


@pytest.mark.parametrize("compressed", [False, True])
@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_measure_from_ova(tmpdir, compressed, fmt):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", "qcow2", compressed=compressed)

    # Measure the image.
    img_measure = client.measure(img, fmt)

    # We don't add member info if member was not specified.
    assert "member-offset" not in img_measure
    assert "member-size" not in img_measure

    # Add test image to ova.
    member = os.path.basename(img)
    ova = str(tmpdir.join("ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(img, arcname=member)

    # Measure the image from the ova.
    ova_measure = client.measure(ova, fmt, member=member)

    # Measurement from ova should be same.
    assert ova_measure["required"] == img_measure["required"]
    assert ova_measure["fully-allocated"] == img_measure["fully-allocated"]

    # If member was specified, we report also the offset and size.
    with tarfile.open(ova) as tar:
        member_info = tar.getmember(member)
    assert ova_measure["member-offset"] == member_info.offset_data
    assert ova_measure["member-size"] == member_info.size


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_checksum(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Create test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    expected = blkhash.checksum(tmp, block_size=1024**2)
    actual = client.checksum(img, block_size=1024**2)
    assert actual == expected


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_checksum_from_ova(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Create test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    # Add test image to ova.
    member = os.path.basename(img)
    ova = str(tmpdir.join("ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(img, arcname=member)

    expected = blkhash.checksum(tmp, block_size=1024**2)
    actual = client.checksum(ova, member=member, block_size=1024**2)
    assert actual == expected


@pytest.mark.parametrize("algorithm,digest_size", [
    ("blake2b", 32),
    ("sha1", None),
])
def test_checksum_algorithm(tmpdir, algorithm, digest_size):
    img = str(tmpdir.join("img"))
    qemu_img.create(img, "raw", size="2m")

    expected = blkhash.checksum(
        img, block_size=1024**2, algorithm=algorithm, digest_size=digest_size)
    actual = client.checksum(img, block_size=1024**2, algorithm=algorithm)
    assert actual == expected


def test_zero_extents_raw(tmpdir):
    size = 10 * 1024**2

    # Create image with some data, zero and holes.
    image = str(tmpdir.join("image.raw"))
    qemu_img.create(image, "raw", size=size)
    with qemu_nbd.open(image, "raw") as c:
        c.write(0 * CLUSTER_SIZE, b"A" * CLUSTER_SIZE)
        c.zero(1 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.write(2 * CLUSTER_SIZE, b"B" * CLUSTER_SIZE)
        c.flush()

    extents = list(client.extents(image))

    # Unallocated area in raw image is not reported as a hole.
    assert extents == [
        ZeroExtent(
            start=0 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=False,
            hole=False),
        ZeroExtent(
            start=1 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=True,
            hole=False),
        ZeroExtent(
            start=2 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=False,
            hole=False),
        ZeroExtent(
            start=3 * CLUSTER_SIZE,
            length=size - 3 * CLUSTER_SIZE,
            zero=True,
            hole=False),
    ]


def test_zero_extents_qcow2(tmpdir):
    size = 10 * 1024**2

    # Create base image with one data and one zero cluster.
    base = str(tmpdir.join("base.qcow2"))
    qemu_img.create(base, "qcow2", size=size)
    with qemu_nbd.open(base, "qcow2") as c:
        c.write(0 * CLUSTER_SIZE, b"A" * CLUSTER_SIZE)
        c.zero(1 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.flush()

    # Create top image with one data and one zero cluster.
    top = str(tmpdir.join("top.qcow2"))
    qemu_img.create(
        top, "qcow2", backing_file=base, backing_format="qcow2")
    with qemu_nbd.open(top, "qcow2") as c:
        c.write(3 * CLUSTER_SIZE, b"B" * CLUSTER_SIZE)
        c.zero(4 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.flush()

    extents = list(client.extents(top))

    assert extents == [
        # Extents from base...
        ZeroExtent(
            start=0 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=False,
            hole=False),
        ZeroExtent(
            start=1 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=True,
            hole=False),
        ZeroExtent(
            start=2 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=True,
            hole=True),

        # Extents from top...
        ZeroExtent(
            start=3 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=False,
            hole=False),
        ZeroExtent(
            start=4 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=True,
            hole=False),

        # Rest of unallocated data...
        ZeroExtent(
            start=5 * CLUSTER_SIZE,
            length=size - 5 * CLUSTER_SIZE,
            zero=True,
            hole=True),
    ]


def test_zero_extents_from_ova(tmpdir):
    size = 10 * 1024**2

    # Create image with data, zero and hole clusters.
    disk = str(tmpdir.join("disk.qcow2"))
    qemu_img.create(disk, "qcow2", size=size)
    with qemu_nbd.open(disk, "qcow2") as c:
        c.write(0 * CLUSTER_SIZE, b"A" * CLUSTER_SIZE)
        c.zero(1 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.flush()

    # Create OVA whith this image.
    ova = str(tmpdir.join("vm.ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(disk, arcname=os.path.basename(disk))

    extents = list(client.extents(ova, member="disk.qcow2"))

    assert extents == [
        ZeroExtent(
            start=0 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=False,
            hole=False),

        # Broken since qemu-nbd 6.0.0.
        ZeroExtent(
            start=1 * CLUSTER_SIZE,
            length=CLUSTER_SIZE,
            zero=True,
            hole=False),

        ZeroExtent(
            start=2 * CLUSTER_SIZE,
            length=size - 2 * CLUSTER_SIZE,
            zero=True,
            hole=True),
    ]


def test_dirty_extents(tmpdir):
    size = 1024**2

    # Create base image with empty dirty bitmap.
    base = str(tmpdir.join("base.qcow2"))
    qemu_img.create(base, "qcow2", size=size)
    qemu_img.bitmap_add(base, "b0")

    # Write data, modifying the dirty bitmap.
    with qemu_nbd.open(base, "qcow2") as c:
        c.write(0 * CLUSTER_SIZE, b"A" * CLUSTER_SIZE)
        c.zero(1 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.flush()

    # Create top image with empty dirty bitmap.
    top = str(tmpdir.join("top.qcow2"))
    qemu_img.create(top, "qcow2", backing_file=base, backing_format="qcow2")
    qemu_img.bitmap_add(top, "b0")

    # Write data, modifying the dirty bitmap.
    with qemu_nbd.open(top, "qcow2") as c:
        c.write(3 * CLUSTER_SIZE, b"B" * CLUSTER_SIZE)
        c.zero(4 * CLUSTER_SIZE, CLUSTER_SIZE)
        c.flush()

    dirty_extents = list(client.extents(base, bitmap="b0"))

    expected = [
        # First cluster is dirty data.
        DirtyExtent(
            start=0 * CLUSTER_SIZE,
            length=1 * CLUSTER_SIZE,
            dirty=True,
            zero=False),
        # Second cluster is dirty zero.
        DirtyExtent(
            start=1 * CLUSTER_SIZE,
            length=1 * CLUSTER_SIZE,
            dirty=True,
            zero=True),
        # Third cluster is clean zero.
        DirtyExtent(
            start=2 * CLUSTER_SIZE,
            length=size - 2 * CLUSTER_SIZE,
            dirty=False,
            zero=True),
    ]

    log.debug("base image dirty extents: %s", dirty_extents)
    assert dirty_extents == expected

    dirty_extents = list(client.extents(top, bitmap="b0"))

    # Note: qemu-nbd reports dirty extents only for the top image, but zero
    # extents are read from the base image.
    expected = [
        # First cluster is clean data, read from base image.
        DirtyExtent(
            start=0 * CLUSTER_SIZE,
            length=1 * CLUSTER_SIZE,
            dirty=False,
            zero=False),
        # Second and third clusters are read from base image. Because they are
        # both clean zero, they are merged.
        DirtyExtent(
            start=1 * CLUSTER_SIZE,
            length=2 * CLUSTER_SIZE,
            dirty=False,
            zero=True),
        # Forth cluster is a data extent modified in top image.
        DirtyExtent(
            start=3 * CLUSTER_SIZE,
            length=1 * CLUSTER_SIZE,
            dirty=True,
            zero=False),
        # Fifth cluster is a zero extent modifed in to image.
        DirtyExtent(
            start=4 * CLUSTER_SIZE,
            length=1 * CLUSTER_SIZE,
            dirty=True,
            zero=True),
        # The rest is clean zero extent.
        DirtyExtent(
            start=5 * CLUSTER_SIZE,
            length=size - 5 * CLUSTER_SIZE,
            dirty=False,
            zero=True),
    ]

    log.debug("top image dirty extents: %s", dirty_extents)
    assert dirty_extents == expected


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_stress(srv, nbd_server, tmpdir, fmt):
    size = 10 * 1024**2

    # Create empty source and destination images.
    src = str(tmpdir.join("src." + fmt))
    qemu_img.create(src, fmt, size=size)
    dst = str(tmpdir.join("dst." + fmt))
    qemu_img.create(dst, fmt, size=size)

    # Upload and download the image multiple times. This used to fail randomly
    # when the executor closed the destination backend before it was cloned by
    # the workers.

    nbd_server.image = dst
    nbd_server.fmt = fmt
    nbd_server.start()

    url = prepare_transfer(srv, nbd_server.sock.url(), size=size)

    for i in range(20):
        client.upload(src, url, srv.config.tls.ca_file)
        client.download(url, src, srv.config.tls.ca_file, fmt=fmt)

    nbd_server.stop()


def test_concurrent_downloads(srv, tmpdir):
    # Testing that we can serve 10 conccurent transfers, assuming 4
    # connections per client.
    # https://bugzilla.redhat.com/2066113

    size = 10 * 1024**2

    def download(url, dst):
        client.download(url, dst, srv.config.tls.ca_file)

    downloads = []
    try:
        for i in range(10):
            src = str(tmpdir.join(f"{i:02d}.src.qcow2"))
            qemu_img.create(src, "qcow2", size=size)

            dst = str(tmpdir.join(f"{i:02d}.dst.qcow2"))
            qemu_img.create(dst, "qcow2", size=size)

            sock = str(tmpdir.join(f"{i:02d}.sock"))

            nbd_server = qemu_nbd.Server(src, "qcow2", nbd.UnixAddress(sock))
            url = prepare_transfer(srv, nbd_server.sock.url(), size=size)
            nbd_server.start()

            downloads.append((nbd_server, url, dst))

        with ThreadPoolExecutor(max_workers=10) as executor:
            tasks = [executor.submit(download, url, dst)
                     for _, url, dst in downloads]
            for t in as_completed(tasks):
                t.result()
    finally:
        for nbd_server, _, _ in downloads:
            nbd_server.stop()
