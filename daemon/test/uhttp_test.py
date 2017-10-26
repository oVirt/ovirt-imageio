# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import
from __future__ import print_function

import os
import pprint
import tempfile
import wsgiref.util

from contextlib import closing
from contextlib import contextmanager

import pytest

from ovirt_imageio_common import util
from ovirt_imageio_daemon import uhttp

PKI_DIR = os.path.dirname(__file__)
KEY_FILE = os.path.join(PKI_DIR, "pki/keys/vdsmkey.pem")
CERT_FILE = os.path.join(PKI_DIR, "pki/certs/vdsmcert.pem")


@pytest.fixture(scope="session")
def uhttpserver(request):
    tmp = tempfile.NamedTemporaryFile()
    server = uhttp.UnixWSGIServer(tmp.name, RequestHandler)
    util.start_thread(server.serve_forever, kwargs={"poll_interval": 0.1})
    request.addfinalizer(server.shutdown)
    request.addfinalizer(tmp.close)
    return server


def test_get(uhttpserver):
    uhttpserver.set_app(get)
    with make_connection(uhttpserver) as con:
        con.request("GET", "/")
        resp = con.getresponse()
        log_response(resp)
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/plain"
        assert resp.read() == b"it works"


def test_put(uhttpserver):
    uhttpserver.set_app(echo)
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
    uhttpserver.set_app(sendfile)
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
            uhttp.UnixWSGIServer(sock, RequestHandler)
    finally:
        tmpdir.chmod(0o755)


def get(env, start_response):
    pprint.pprint(env)
    start_response("200 OK", [("content-type", "text/plain")])
    return [b"it works"]


def echo(env, start_response):
    pprint.pprint(env)
    content_length = env["CONTENT_LENGTH"]
    body = env["wsgi.input"].read(int(content_length))
    start_response("200 OK", [
        ("content-type", "text/plain"),
        ("content-length", content_length),
    ])
    return [body]


def sendfile(env, start_response):
    pprint.pprint(env)
    path = env["PATH_INFO"]
    start_response("200 OK", [
        ("content-type", "text/plain"),
        ("content-length", str(os.path.getsize(path)))
    ])
    return wsgiref.util.FileWrapper(open(path, "rb"))


def log_response(resp):
    pprint.pprint((resp.status, resp.reason, resp.getheaders()))


class RequestHandler(uhttp.UnixWSGIRequestHandler):

    protocol_version = "HTTP/1.1"

    def log_request(self, code, size):
        pass

    def log_message(self, fmt, *args):
        print(fmt % args)


@contextmanager
def make_connection(server):
    con = uhttp.UnixHTTPConnection(server.server_address, timeout=2)
    with closing(con):
        yield con
