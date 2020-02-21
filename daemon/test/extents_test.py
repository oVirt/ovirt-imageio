# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import json
import os
import ssl
import subprocess

import userstorage
import pytest

from ovirt_imageio_common import auth
from ovirt_imageio_common import config
from ovirt_imageio_common import configloader
from ovirt_imageio_common import server

from . import testutil
from . import http
from . import storage

BACKENDS = userstorage.load_config("../storage.py").BACKENDS


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


# Disable client certificate verification introduced in Python > 2.7.9. We
# trust our certificates.
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass  # Older Python, not required


@pytest.fixture(scope="module")
def images_server():
    conf = os.path.join(os.path.dirname(__file__), "daemon.conf")
    configloader.load(config, [conf])
    server.start(config)
    yield
    server.stop()


@pytest.fixture(params=[
    pytest.param(http.request, id="http"),
    pytest.param(http.local, id="local"),
])
def http_request(request, images_server):
    auth.clear()
    return request.param


def test_unupported_method(http_request):
    res = http_request("PUT", "/images/no-such-ticket/extents")
    assert res.status == 405


def test_no_ticket_id(http_request):
    res = http_request("GET", "/images//extents")
    assert res.status == 400


def test_no_ticket(http_request):
    res = http_request("GET", "/images/no-such-ticket/extents")
    assert res.status == 403


def test_ticket_expired(http_request, fake_time):
    ticket = testutil.create_ticket(timeout=300)
    auth.add(ticket)

    # Make the ticket expire
    fake_time.now += 300

    res = http_request("GET", "/images/%(uuid)s/extents" % ticket)
    assert res.status == 403


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
@pytest.mark.parametrize("path", [
    "/images/%(uuid)s/extents",
    "/images/%(uuid)s/extents?context=zero",
])
def test_file_zero(http_request, user_file, fmt, path):
    subprocess.check_call(
        ["qemu-img", "create", "-f", fmt, user_file.path, "1g"])

    # File backend uses actual size, not vitual size.
    size = os.path.getsize(user_file.path)

    ticket = testutil.create_ticket(url="file://" + user_file.path, size=size)
    auth.add(ticket)

    res = http_request("GET", path % ticket)
    data = res.read()
    assert res.status == 200

    extents = json.loads(data.decode("utf-8"))
    assert extents == [{"start": 0, "length": size, "zero": False}]


def test_file_ticket_not_dirty(http_request, tmpfile):
    with open(str(tmpfile), "wb") as f:
        f.truncate(65536)

    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile), size=65536, dirty=False)
    auth.add(ticket)

    res = http_request(
        "GET", "/images/%(uuid)s/extents?context=dirty" % ticket)
    res.read()
    assert res.status == 404


def test_file_does_not_support_dirty(http_request, tmpfile):
    with open(str(tmpfile), "wb") as f:
        f.truncate(65536)

    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile), size=65536, dirty=True)
    auth.add(ticket)

    res = http_request(
        "GET", "/images/%(uuid)s/extents?context=dirty" % ticket)
    res.read()
    assert res.status == 404
