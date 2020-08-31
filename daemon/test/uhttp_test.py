# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import os
import pprint
import tempfile

from contextlib import closing
from contextlib import contextmanager

import pytest

from ovirt_imageio._internal import util
from ovirt_imageio._internal import uhttp


@pytest.fixture(scope="session")
def uhttpserver(request):
    tmp = tempfile.NamedTemporaryFile()
    server = uhttp.Server(tmp.name, uhttp.Connection)
    util.start_thread(server.serve_forever, kwargs={"poll_interval": 0.1})
    request.addfinalizer(server.shutdown)
    request.addfinalizer(tmp.close)
    return server


def test_get(uhttpserver):
    uhttpserver.app = get
    with make_connection(uhttpserver) as con:
        con.request("GET", "/")
        resp = con.getresponse()
        log_response(resp)
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/plain"
        assert resp.read() == b"it works"


def test_put(uhttpserver):
    uhttpserver.app = echo
    with make_connection(uhttpserver) as con:
        con.request("PUT", "/", body=b"it works")
        resp = con.getresponse()
        log_response(resp)
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/plain"
        assert resp.read() == b"it works"


def test_file(tmpdir, uhttpserver):
    data = b"x" * 1048576
    tmp = tmpdir.join("data")
    tmp.write(data)
    uhttpserver.app = sendfile
    with make_connection(uhttpserver) as con:
        con.request("GET", str(tmp))
        resp = con.getresponse()
        log_response(resp)
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/plain"
        content_length = int(resp.getheader("content-length"))
        assert content_length == os.path.getsize(str(tmp))
        assert resp.read(content_length) == data


def test_connection_set_tunnel(uhttpserver):
    with make_connection(uhttpserver) as con:
        with pytest.raises(uhttp.UnsupportedError):
            con.set_tunnel("127.0.0.1")


@pytest.mark.skipif(os.geteuid() == 0,
                    reason="Not compatible when running with root")
def test_server_bind_error(tmpdir):
    # Make server_bind fail with EPERM
    tmpdir.chmod(0o600)
    try:
        sock = str(tmpdir.join('sock'))
        with pytest.raises(OSError):
            uhttp.Server(sock, uhttp.Connection)
    finally:
        tmpdir.chmod(0o755)


def get(req, resp):
    body = b"it works"
    resp.headers["content-length"] = len(body)
    resp.headers["content-type"] = "text/plain"
    resp.write(body)


def echo(req, resp):
    body = req.read()
    resp.headers["content-length"] = len(body)
    resp.headers["content-type"] = "text/plain"
    resp.write(body)


def sendfile(req, resp):
    path = req.path
    resp.headers["content-length"] = os.path.getsize(path)
    resp.headers["content-type"] = "text/plain"
    with open(path, "rb") as f:
        resp.write(f.read())


def log_response(resp):
    pprint.pprint((resp.status, resp.reason, resp.getheaders()))


@contextmanager
def make_connection(server):
    con = uhttp.UnixHTTPConnection(server.server_address, timeout=2)
    with closing(con):
        yield con
