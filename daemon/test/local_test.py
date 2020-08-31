# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import http.client as http_client
import io
import json
import os
import stat

import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import server

from . import http
from . import testutil


@pytest.fixture(scope="module")
def srv():
    cfg = config.load(["test/conf/daemon.conf"])
    s = server.Server(cfg)
    s.start()
    try:
        yield s
    finally:
        s.stop()


def test_method_not_allowed(srv):
    with http.LocalClient(srv.config) as c:
        res = c.request("FOO", "/images/")
        assert res.status == http_client.METHOD_NOT_ALLOWED


@pytest.mark.parametrize("method", ["GET", "PUT", "PATCH"])
def test_no_resource(srv, method):
    with http.LocalClient(srv.config) as c:
        res = c.request(method, "/no/resource")
        assert res.status == http_client.NOT_FOUND


@pytest.mark.parametrize("method", ["GET", "PUT", "PATCH"])
def test_no_ticket_id(srv, method):
    with http.LocalClient(srv.config) as c:
        res = c.request(method, "/images/")
        assert res.status == http_client.BAD_REQUEST


@pytest.mark.parametrize("method,body", [
    ("GET", None),
    ("PUT", "body"),
    ("PATCH", json.dumps({"op": "flush"}).encode("ascii")),
])
def test_no_ticket(srv, method, body):
    with http.LocalClient(srv.config) as c:
        res = c.request(method, "/images/no-ticket", body=body)
        assert res.status == http_client.FORBIDDEN


def test_put_forbidden(srv):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=["read"])
    srv.auth.add(ticket)
    with http.LocalClient(srv.config) as c:
        res = c.put("/images/" + ticket["uuid"], "content")
        assert res.status == http_client.FORBIDDEN


def test_put(srv, tmpdir):
    data = b"-------|after"
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    uri = "/images/" + ticket["uuid"]
    with http.LocalClient(srv.config) as c:
        res = c.put(uri, "content")
        assert res.status == http_client.OK
        assert res.getheader("content-length") == "0"
        with io.open(str(image)) as f:
            assert f.read(len(data)) == "content|after"


def test_get_forbidden(srv):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=[])
    srv.auth.add(ticket)
    with http.LocalClient(srv.config) as c:
        res = c.get("/images/" + ticket["uuid"], {"header": "content"})
        assert res.status == http_client.FORBIDDEN


def test_get(srv, tmpdir):
    data = b"a" * 512 + b"b" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=1024)
    srv.auth.add(ticket)
    with http.LocalClient(srv.config) as c:
        res = c.get("/images/" + ticket["uuid"])
        assert res.status == http_client.OK
        assert res.read() == data


def test_images_zero(srv, tmpdir):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    msg = {"op": "zero", "size": 20, "offset": 10, "future": True}
    size = msg["size"]
    offset = msg.get("offset", 0)
    body = json.dumps(msg).encode("ascii")
    with http.LocalClient(srv.config) as c:
        res = c.patch("/images/" + ticket["uuid"], body)
        assert res.status == http_client.OK
        assert res.getheader("content-length") == "0"
        with io.open(str(image), "rb") as f:
            assert f.read(offset) == data[:offset]
            assert f.read(size) == b"\0" * size
            assert f.read() == data[offset + size:]


def test_options(srv):
    with http.LocalClient(srv.config) as c:
        res = c.options("/images/*")
        allows = {"OPTIONS", "GET", "PUT", "PATCH"}
        features = {"checksum", "extents", "flush", "zero"}
        assert res.status == http_client.OK
        assert set(res.getheader("allow").split(',')) == allows
        options = json.loads(res.read())
        assert set(options["features"]) == features
        assert options["unix_socket"] == srv.local_service.address


def test_control_socket_mode(srv):
    socket = srv.config.control.socket
    actual_mode = stat.S_IMODE(os.stat(socket).st_mode)
    assert oct(actual_mode) == oct(0o660)
