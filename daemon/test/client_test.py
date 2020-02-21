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

from ovirt_imageio import auth
from ovirt_imageio import config
from ovirt_imageio import configloader
from ovirt_imageio import client
from ovirt_imageio import pki
from ovirt_imageio import server

from . import testutil

from . marks import requires_python3

pytestmark = requires_python3

IMAGE_SIZE = 128 * 1024


def setup_module(m):
    conf = os.path.join(os.path.dirname(__file__), "daemon.conf")
    configloader.load(config, [conf])
    server.start(config)


def teardown_module(m):
    server.stop()


def setup_function(f):
    auth.clear()


def check_content(src, dst):
    with open(src, "rb") as s, open(dst, "rb") as d:
        assert s.read() == d.read()


def prepare_upload(dst, sparse=True, size=IMAGE_SIZE):
    with open(dst, "wb") as f:
        if not sparse:
            f.write(b"a" * size)

    ticket = testutil.create_ticket(
        url="file://" + dst,
        size=size,
        sparse=sparse)

    auth.add(ticket)

    return "https://localhost:{}/images/{}".format(
        server.remote_service.port, ticket["uuid"])


class FakeProgress:

    def __init__(self, size=0):
        self.size = size
        self.updates = []

    def update(self, n):
        self.updates.append(n)


# TODO:
# - All tests use secure=False to workaround our bad certificates. Once we fix
#   the certificates we need to test with the default secure=True.
# - verify that upload optimized the upload using unix socket. Need a way to
#   enable only OPTIONS on the remote server.
# - verify that upload fall back to HTTPS if server does not support unix
#   socket. We don't have a way to disable unix socket currently.
# - verify that upload fall back to HTTPS if server support unix socket but is
#   not the local host. Probbly not feasble for these tests, unless we can
#   start a daemon on another host.
# - Test negative flows


def test_upload_empty_sparse(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst)

    client.upload(src, url, pki.cert_file(config), secure=False)

    check_content(src, dst)
    assert os.stat(dst).st_blocks == os.stat(src).st_blocks


def test_upload_hole_at_start_sparse(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"b" * (IMAGE_SIZE // 2))

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst)

    client.upload(src, url, pki.cert_file(config), secure=False)

    check_content(src, dst)
    assert os.stat(dst).st_blocks == os.stat(src).st_blocks


def test_upload_hole_at_middle_sparse(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.write(b"b" * (IMAGE_SIZE // 4))
        f.seek(IMAGE_SIZE // 2, os.SEEK_CUR)
        f.write(b"b" * (IMAGE_SIZE // 4))

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst)

    client.upload(src, url, pki.cert_file(config), secure=False)

    check_content(src, dst)
    assert os.stat(dst).st_blocks == os.stat(src).st_blocks


def test_upload_hole_at_end_sparse(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.write(b"b" * (IMAGE_SIZE // 2))

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst)

    client.upload(src, url, pki.cert_file(config), secure=False)

    check_content(src, dst)
    assert os.stat(dst).st_blocks == os.stat(src).st_blocks


def test_upload_full_sparse(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.write(b"b" * IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst)

    client.upload(src, url, pki.cert_file(config), secure=False)

    check_content(src, dst)
    assert os.stat(dst).st_blocks == os.stat(src).st_blocks


def test_upload_preallocated(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst, sparse=False)

    client.upload(src, url, pki.cert_file(config), secure=False)

    check_content(src, dst)
    assert os.stat(dst).st_blocks * 512 == IMAGE_SIZE


@pytest.mark.parametrize("use_unix_socket", [True, False])
def test_upload_unix_socket(tmpdir, use_unix_socket):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.write(b"b" * IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst)

    client.upload(src, url, pki.cert_file(config), secure=False)

    check_content(src, dst)


def test_progress(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.write(b"b" * 4096)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"b" * 4096)
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst, sparse=True)

    progress = FakeProgress(IMAGE_SIZE)
    client.upload(src, url, pki.cert_file(config), secure=False,
                  progress=progress)

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


def test_progress_callback(tmpdir):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst, size=IMAGE_SIZE, sparse=True)

    progress = []
    client.upload(src, url, pki.cert_file(config), secure=False,
                  progress=progress.append)

    assert progress == [IMAGE_SIZE]
