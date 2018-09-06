# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import os
import sys

import pytest

from ovirt_imageio_common import configloader
from ovirt_imageio_common import client
from ovirt_imageio_daemon import config
from ovirt_imageio_daemon import server
from ovirt_imageio_daemon import tickets
from ovirt_imageio_daemon import pki

from . import testutils

pytestmark = pytest.mark.skipif(sys.version_info[0] > 2,
                                reason='needs porting to python 3')

IMAGE_SIZE = 128 * 1024


def setup_module(m):
    conf = os.path.join(os.path.dirname(__file__), "daemon.conf")
    configloader.load(config, [conf])
    server.start(config)


def teardown_module(m):
    server.stop()


def setup_function(f):
    tickets.clear()


def check_content(src, dst):
    with open(src, "rb") as s, open(dst, "rb") as d:
        assert s.read() == d.read()


def prepare_upload(dst, sparse=True, size=IMAGE_SIZE):
    with open(dst, "wb") as f:
        if not sparse:
            f.write(b"a" * size)

    ticket = testutils.create_ticket(
        url="file://" + dst,
        size=size,
        sparse=sparse)

    tickets.add(ticket)

    return "https://localhost:{}/images/{}".format(
        server.remote_service.port, ticket["uuid"])


# TODO:
# - All tests use secure=False to workaround our bad certificates. Once we fix
#   the certificates we need to test with the default secure=True.
# - test server not supporting unix socket. We don't have a way to disable unix
#   socket currently.
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

    client.upload(src, url, pki.cert_file(config), secure=False,
                  use_unix_socket=use_unix_socket)

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

    progress = []
    client.upload(src, url, pki.cert_file(config), secure=False,
                  progress=progress.append)

    assert progress == [
        # First write.
        4096,
        # First zero.
        IMAGE_SIZE // 2 - 4096,
        # Second write.
        4096,
        # Second zero
        IMAGE_SIZE // 2 - 4096,
    ]


def test_split_big_zero(tmpdir):
    # Large zero ranges shhould be splitted to smaller chunks.
    size = client.MAX_ZERO_SIZE * 2 + client.MAX_ZERO_SIZE // 2
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(size)

    dst = str(tmpdir.join("dst"))
    url = prepare_upload(dst, size=size, sparse=True)

    progress = []
    client.upload(src, url, pki.cert_file(config), secure=False,
                  progress=progress.append)

    assert progress == [
        client.MAX_ZERO_SIZE,
        client.MAX_ZERO_SIZE,
        client.MAX_ZERO_SIZE // 2,
    ]
