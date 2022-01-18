# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
import os
import subprocess

import userstorage
import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import server

from .. import testutil
from .. import http
from .. import storage

BACKENDS = userstorage.load_config("storage.py").BACKENDS


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


def test_unupported_method(srv, client):
    res = client.request("PUT", "/images/no-such-ticket/extents")
    assert res.status == 405


def test_no_ticket_id(srv, client):
    res = client.request("GET", "/images//extents")
    assert res.status == 400


def test_no_ticket(srv, client):
    res = client.request("GET", "/images/no-such-ticket/extents")
    assert res.status == 403


def test_ticket_expired(srv, client, fake_time):
    ticket = testutil.create_ticket(timeout=300)
    srv.auth.add(ticket)

    # Make the ticket expire
    fake_time.now += 300

    res = client.request("GET", "/images/%(uuid)s/extents" % ticket)
    assert res.status == 403


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
@pytest.mark.parametrize("path", [
    "/images/%(uuid)s/extents",
    "/images/%(uuid)s/extents?context=zero",
])
def test_file_zero(srv, client, user_file, fmt, path):
    subprocess.check_call(
        ["qemu-img", "create", "-f", fmt, user_file.path, "1g"])

    # File backend uses actual size, not vitual size.
    size = os.path.getsize(user_file.path)

    ticket = testutil.create_ticket(url="file://" + user_file.path, size=size)
    srv.auth.add(ticket)

    res = client.request("GET", path % ticket)
    data = res.read()
    assert res.status == 200

    extents = json.loads(data.decode("utf-8"))
    assert extents == [
        {"start": 0, "length": size, "zero": False, "hole": False}
    ]


def test_file_ticket_not_dirty(srv, client, tmpfile):
    with open(str(tmpfile), "wb") as f:
        f.truncate(65536)

    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile), size=65536, dirty=False)
    srv.auth.add(ticket)

    res = client.request(
        "GET", "/images/%(uuid)s/extents?context=dirty" % ticket)
    res.read()
    assert res.status == 404


def test_file_does_not_support_dirty(srv, client, tmpfile):
    with open(str(tmpfile), "wb") as f:
        f.truncate(65536)

    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile), size=65536, dirty=True)
    srv.auth.add(ticket)

    res = client.request(
        "GET", "/images/%(uuid)s/extents?context=dirty" % ticket)
    res.read()
    assert res.status == 404
