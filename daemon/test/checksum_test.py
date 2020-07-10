# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import hashlib
import json
import os

import userstorage
import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import server

from . import testutil
from . import http
from . import storage

BACKENDS = userstorage.load_config("../storage.py").BACKENDS
ALGORITHMS = frozenset(hashlib.algorithms_available)


@pytest.fixture(
    params=[
        BACKENDS["file-512-xfs"],
        BACKENDS["file-4k-xfs"],
    ],
    ids=str
)
def user_file(request):
    with storage.Backend(request.param) as backend:
        yield backend


@pytest.fixture(scope="module")
def srv():
    cfg = config.load(["test/conf/daemon.conf"])
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


@pytest.fixture(params=[
    pytest.param(http.RemoteClient, id="http"),
    pytest.param(http.LocalClient, id="local"),
])
def client(srv, request):
    srv.auth.clear()
    client = request.param(srv.config)
    yield client
    client.close()


def test_unsupported_method(srv, client):
    res = client.request("PUT", "/images/ticket-id/checksum")
    assert res.status == 405


def test_no_ticket_id(srv, client):
    res = client.request("GET", "/images//checksum")
    assert res.status == 400


def test_no_ticket(srv, client):
    res = client.request("GET", "/images/no-such-ticket/checksum")
    assert res.status == 403


def test_ticket_expired(srv, client, fake_time):
    ticket = testutil.create_ticket(timeout=300)
    srv.auth.add(ticket)

    # Make the ticket expire
    fake_time.now += 300

    res = client.request("GET", "/images/%(uuid)s/checksum" % ticket)
    assert res.status == 403


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_file_backend(srv, client, user_file, fmt):
    qemu_img.create(user_file.path, fmt, size="2m")

    with qemu_nbd.open(user_file.path, fmt) as c:
        # Add cluster with data.
        c.write(1 * 1024**2, b"some data")
        c.flush()

    # File backend operate on host data, not guest data.
    with open(user_file.path, "rb") as f:
        checksum = hashlib.sha1(f.read()).hexdigest()

    # File backend uses actual size, not vitual size.
    size = os.path.getsize(user_file.path)

    ticket = testutil.create_ticket(url="file://" + user_file.path, size=size)
    srv.auth.add(ticket)

    res = client.request("GET", "/images/{}/checksum".format(ticket["uuid"]))
    data = res.read()
    assert res.status == 200

    res = json.loads(data)
    assert res == {"checksum": checksum, "algorithm": "sha1"}


@pytest.mark.parametrize("fmt,compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_nbd_backend(srv, client, tmpdir, nbd_server, fmt, compressed):
    size = 2 * 1024**2

    # Create temporary file with some data.
    tmp = tmpdir.join("tmp")
    with open(tmp, "wb") as f:
        f.truncate(size)
        # Add zero allocated cluster.
        f.write(b"\0" * 64 * 1024)
        # Add cluster with data.
        f.seek(1 * 1024**2)
        f.write(b"some data")

    # NBD backend checksums guest visible data.
    with open(tmp, "rb") as f:
        checksum = hashlib.sha1(f.read()).hexdigest()

    # Create test image.
    qemu_img.convert(tmp, nbd_server.image, "raw", fmt, compressed=compressed)

    nbd_server.fmt = fmt
    nbd_server.start()

    ticket = testutil.create_ticket(url=nbd_server.sock.url(), size=size)
    srv.auth.add(ticket)

    res = client.request("GET", "/images/{}/checksum".format(ticket["uuid"]))
    data = res.read()
    assert res.status == 200

    res = json.loads(data)
    assert res == {"checksum": checksum, "algorithm": "sha1"}


@pytest.mark.parametrize("algorithm", ["sha1", "sha256"])
def test_algorithms(srv, client, tmpdir, algorithm):
    size = 2 * 1024**2
    image = str(tmpdir.join("image"))
    qemu_img.create(image, "raw", size=size)
    ticket = testutil.create_ticket(url="file://" + image, size=size)
    srv.auth.add(ticket)

    # File backend operate on host data, not guest data.
    with open(image, "rb") as f:
        checksum = hashlib.new(algorithm, f.read()).hexdigest()

    res = client.request(
        "GET",
        "/images/{}/checksum?algorithm={}"
        .format(ticket["uuid"], algorithm))
    data = res.read()
    assert res.status == 200

    res = json.loads(data)
    assert res == {"checksum": checksum, "algorithm": algorithm}


def test_algorithms_unknown(srv, client, tmpdir):
    ticket = testutil.create_ticket(url="file:///no/such/file", size=4096)
    srv.auth.add(ticket)
    res = client.request(
        "GET",
        "/images/{}/checksum?algorithm=unknown".format(ticket["uuid"]))
    error = res.read().decode("utf-8")
    assert res.status == 400
    assert repr("unknown") in error
    assert repr("algorithm") in error
    for name in ALGORITHMS:
        assert repr(name) in error


def test_checksum_algorithms(srv, client, tmpdir):
    res = client.request("GET", "/images/no-ticket/checksum/algorithms")
    data = res.read()
    assert res.status == 200

    res = json.loads(data)
    assert res == {"algorithms": sorted(ALGORITHMS)}
