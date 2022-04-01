# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import io
import logging
import os
import subprocess
import urllib.parse

from collections import namedtuple

import pytest

from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import util

log = logging.getLogger("test")


@pytest.fixture
def tmpfile(tmpdir):
    """
    Return a path to an empty temporary file.
    """
    f = tmpdir.join("tmpfile")
    f.write("")
    return str(f)


@pytest.fixture
def tmpurl(tmpfile):
    """
    Return a file: url to an empty temporary file.
    """
    return urllib.parse.urlparse("file:" + tmpfile)


@pytest.fixture
def nbd_server(tmpdir):
    """
    Returns nbd_server exporting a temporary file.

    The test may configure the server before starting it. Typical example is
    setting the read_only flag to create a read only server.

    The test must start the server. The test framework will stop the server
    when the test ends.
    """
    image = str(tmpdir.join("image"))
    with io.open(image, "wb") as f:
        f.truncate(10 * 1024**2)

    sock = nbd.UnixAddress(tmpdir.join("sock"))

    server = qemu_nbd.Server(image, "raw", sock)
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def base_image():
    path = "/var/tmp/imageio-images/cirros-0.3.5.img"
    if not os.path.exists(path):
        pytest.skip(f"{path} not available")

    return path


# Arguments to ssl.{server,client}_context()
PKI = namedtuple("PKI", "cafile,certfile,keyfile")


@pytest.fixture(scope="session")
def tmp_pki(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("pki")
    certfile = str(tmpdir.join("cert.pem"))
    keyfile = str(tmpdir.join("key.pem"))

    cmd = [
        "openssl", "req",
        "-new",
        "-x509",
        "-nodes",
        "-batch",
        "-days", "2",
        "-subj", "/CN=localhost",
        "-out", certfile,
        "-keyout", keyfile,
    ]

    subprocess.check_output(cmd)

    return PKI(certfile, certfile, keyfile)


class FakeTime:

    def __init__(self):
        self.now = 0

    def monotonic_time(self):
        return self.now


@pytest.fixture
def fake_time(monkeypatch):
    """
    Monkeypatch util.monotonic_time for testing time related operations.

    Returns FakeTime instance. Modifying instance.now change the value returned
    from the monkeypatched util.monotonic_time().
    """
    time = FakeTime()
    monkeypatch.setattr(util, "monotonic_time", time.monotonic_time)
    return time
