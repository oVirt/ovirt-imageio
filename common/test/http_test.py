# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import logging
import time

from contextlib import closing

from six.moves import http_client

import pytest

from ovirt_imageio_common import http
from ovirt_imageio_common import util

log = logging.getLogger("test")


class Demo(object):

    def get(self, req, resp, name):
        body = b"%s\n" % name.encode("utf-8")
        resp.headers["content-length"] = str(len(body))
        resp.write(body)

    def delete(self, req, resp, name):
        resp.status_code = 204

    def options(self, req, resp, name):
        resp.status_code = 200
        resp.headers["content-length"] = "0"
        resp.headers["allow"] = "GET,DELETE,OPTIONS"


class Echo(object):

    def put(self, req, resp, ticket):
        if req.headers.get("expect") == "100-continue":
            resp.send_info(100)

        count = int(req.headers["content-length"])
        resp.headers["content-length"] = str(count)

        while count:
            chunk = req.read(min(count, 1024 * 1024))
            if not chunk:
                raise http.Error(400, "Client disconnected")
            resp.write(chunk)
            count -= len(chunk)


class Context(object):
    """
    Keep per-connection state example.
    """

    def put(self, req, resp, name):
        count = int(req.headers["content-length"])
        value = req.read(count)
        req.context[name] = value
        resp.status_code = 200
        resp.headers["content-length"] = "0"

    def get(self, req, resp, name):
        if name not in req.context:
            raise http.Error(404, "No such name {!r}".format(name))
        value = req.context[name]
        resp.headers["content-length"] = len(value)
        resp.write(value)

    def delete(self, req, resp, name):
        req.context.pop(name, None)
        resp.status_code = 204


class Closeable(object):

    def __init__(self, name, log):
        self.name = name
        self.log = log

    def close(self):
        self.log.write(u"{} was closed\n".format(self.name))
        # For checking that all objects are closed when a connection is closed.
        raise RuntimeError(u"Error closing {!r}".format(self.name))


class CloseContext(object):
    """
    Example for closing objects when connection is closed.
    """

    def __init__(self):
        self.log = io.StringIO()

    def put(self, req, resp, name):
        req.context[name] = Closeable(name, self.log)
        resp.status_code = 200
        resp.headers["content-length"] = "0"

    def get(self, req, resp, *args):
        value = self.log.getvalue().encode("utf-8")
        self.log = io.StringIO()
        resp.headers["content-length"] = len(value)
        resp.write(value)


class ServerError(object):

    def get(self, req, resp, name):
        raise RuntimeError("secret data")

    def put(self, req, resp, name):
        # Raising without reading payload wil fail with EPIPE on the
        # client side. If the client is careful, it will get error 500.
        raise RuntimeError("secret data")


class ClientError(object):

    def get(self, req, resp, name):
        raise http.Error(403, "No data for you!")

    def put(self, req, resp, name):
        # Raising without reading payload wil fail with EPIPE on the
        # client side. If the client is careful, it will get error 403.
        raise http.Error(403, "No data for you!")


@pytest.fixture(scope="module")
def server():
    server = http.Server(("", 0), http.Connection)
    log.info("Server listening on port %d", server.server_port)

    server.app = http.Router([
        (r"/demo/(.*)", Demo()),
        (r"/echo/(.*)", Echo()),
        (r"/context/(.*)", Context()),
        (r"/close-context/(.*)", CloseContext()),
        (r"/server-error/(.*)", ServerError()),
        (r"/client-error/(.*)", ClientError()),
    ])

    t = util.start_thread(
        server.serve_forever,
        kwargs={"poll_interval": 0.1})
    try:
        yield server
    finally:
        server.shutdown()
        t.join()


def test_demo_get(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/demo/name")
        r = con.getresponse()
        assert r.status == 200
        assert r.read() == b"name\n"


def test_demo_get_empty(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/demo/")
        r = con.getresponse()
        assert r.status == 200
        assert r.read() == b"\n"


def test_demo_delete(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("DELETE", "/demo/name")
        r = con.getresponse()
        assert r.status == 204
        assert r.read() == b""


def test_demo_options(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("OPTIONS", "/demo/name")
        r = con.getresponse()
        assert r.status == 200
        assert r.getheader("allow") == "GET,DELETE,OPTIONS"
        assert r.read() == b""


@pytest.mark.parametrize("data", [b"it works!", b""])
def test_echo(server, data):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("PUT", "/echo/test", body=data)
        r = con.getresponse()
        assert r.status == 200
        assert r.read() == data


def test_echo_100_continue(server):
    data = b"it works!"
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request(
            "PUT",
            "/echo/test",
            body=data,
            headers={"expect": "100-continue"})
        r = con.getresponse()
        assert r.status == 200
        assert r.read() == data


def test_context(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # No context yet.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == 404
        r.read()

        # Set value for "this".
        con.request("PUT", "/context/this", body=b"value")
        r = con.getresponse()
        assert r.status == 200
        r.read()

        # Should have value now.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == 200
        assert r.read() == b"value"

        # Remove value for "this".
        con.request("DELETE", "/context/this")
        r = con.getresponse()
        assert r.status == 204
        r.read()

        # No context now.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == 404
        r.read()


def test_context_per_connection(server):
    con1 = http_client.HTTPConnection("localhost", server.server_port)
    con2 = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con1), closing(con2):
        # Set value for "this" in connection 1.
        con1.request("PUT", "/context/this", body=b"con1 value")
        r = con1.getresponse()
        assert r.status == 200
        r.read()

        # Connection 1 should have no value.
        con2.request("GET", "/context/this")
        r = con2.getresponse()
        assert r.status == 404
        r.read()

        # Set value for "this" in connection 2.
        con2.request("PUT", "/context/this", body=b"con2 value")
        r = con2.getresponse()
        assert r.status == 200
        r.read()

        # Connection 1 value did not change.
        con1.request("GET", "/context/this")
        r = con1.getresponse()
        assert r.status == 200
        assert r.read() == b"con1 value"


def test_context_deleted_on_close(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Set value for "this".
        con.request("PUT", "/context/this", body=b"con value")
        r = con.getresponse()
        assert r.status == 200
        r.read()

    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Should have no value.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == 404
        r.read()


def test_context_close(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Add a closable object to the connection context
        con.request("PUT", "/close-context/foo")
        r = con.getresponse()
        assert r.status == 200
        r.read()

    # Run server thread to detect the close.
    time.sleep(0.1)

    # Closing the connection should close the object.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/close-context/")
        r = con.getresponse()
        assert r.status == 200
        log = r.read().decode("utf-8")
        assert "foo was closed" in log


def test_context_close_multiple_objects(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Add a closable object to the connection context
        con.request("PUT", "/close-context/foo")
        r = con.getresponse()
        assert r.status == 200
        r.read()

        # Add another
        con.request("PUT", "/close-context/bar")
        r = con.getresponse()
        assert r.status == 200
        r.read()

    # Run server thread to detect the close.
    time.sleep(0.1)

    # Closing the connection should close both objects.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/close-context/")
        r = con.getresponse()
        assert r.status == 200
        log = r.read().decode("utf-8")
        assert "foo was closed" in log
        assert "bar was closed" in log


def test_not_found(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/no/such/path")
        r = con.getresponse()
        assert r.status == 404


def test_method_not_allowed(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("POST", "/demo/name")
        r = con.getresponse()
        assert r.status == 405


def test_invalid_method(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("FOO", "/demo/name")
        r = con.getresponse()
        assert r.status == 405


def test_client_error_get(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/client-error/")
        r = con.getresponse()
        assert r.status == 403


def test_client_error_put(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("PUT", "/client-error/", body=b"x" * 1024**2)
        r = con.getresponse()
        assert r.status == 403


def test_internal_error_get(server):
    # Internal error should not expose secret data in client response.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/server-error/")
        r = con.getresponse()
        assert r.status == 500
        assert "secret" not in r.read().decode("utf-8")


def test_internal_error(server):
    # Internal error should not expose secret data in client response.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("PUT", "/server-error/", body=b"x" * 1024**2)
        r = con.getresponse()
        assert r.status == 500
        assert "secret" not in r.read().decode("utf-8")
