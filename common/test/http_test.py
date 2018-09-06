# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import io
import json
import logging
import socket
import time

from contextlib import closing

import six
from six.moves import http_client

import pytest

from ovirt_imageio_common import http
from ovirt_imageio_common import util

log = logging.getLogger("test")


class Demo(object):

    def get(self, req, resp, name):
        body = b"%s\n" % name.encode("utf-8")
        resp.headers["content-length"] = len(body)
        resp.write(body)

    def delete(self, req, resp, name):
        resp.status_code = http.NO_CONTENT

    def options(self, req, resp, name):
        resp.headers["content-length"] = 0
        resp.headers["allow"] = "GET,DELETE,OPTIONS"


class Echo(object):

    def put(self, req, resp, ticket):
        if req.headers.get("expect") == "100-continue":
            resp.send_info(http.CONTINUE)

        count = req.content_length
        resp.headers["content-length"] = count

        while count:
            chunk = req.read(1024 * 1024)
            if not chunk:
                raise http.Error(http.BAD_REQUEST, "Client disconnected")
            resp.write(chunk)
            count -= len(chunk)


class RequestInfo(object):

    def get(self, req, resp, arg=None):
        self.send_response(req, resp, arg)

    def put(self, req, resp, arg=None):
        body = req.read().decode("utf-8")
        self.send_response(req, resp, arg, body)

    def send_response(self, req, resp, arg, content=None):
        # Python 2.7 returns lowercase keys, Python 3.6 keeps original
        # case. Since headers are case insensitive, lets normalize both to
        # lowercase.
        headers = dict((k.lower(), req.headers[k]) for k in req.headers)

        # Ensure that req.query return decoded values. This is hard to test
        # using json response, since json accepts both text and bytes and
        # generate a bytestream that decodes to unicode on the other side.
        for k, v in six.iteritems(req.query):
            assert type(k) == six.text_type
            assert type(v) == six.text_type

        info = {
            "method": req.method,
            "uri": req.uri,
            "arg": arg,
            "path": req.path,
            "query": req.query,
            "version": req.version,
            "content_length": req.content_length,
            "content": content,
            "headers": headers,
            "client_addr": req.client_addr,
        }
        body = json.dumps(info).encode("utf-8")
        resp.headers["content-length"] = len(body)
        resp.write(body)


class Context(object):
    """
    Keep per-connection state example.
    """

    def put(self, req, resp, name):
        value = req.read()
        req.context[name] = value
        resp.headers["content-length"] = 0

    def get(self, req, resp, name):
        if name not in req.context:
            raise http.Error(http.NOT_FOUND, "No such name {!r}".format(name))
        value = req.context[name]
        resp.headers["content-length"] = len(value)
        resp.write(value)

    def delete(self, req, resp, name):
        req.context.pop(name, None)
        resp.status_code = http.NO_CONTENT


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
        resp.headers["content-length"] = 0

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
        raise http.Error(http.FORBIDDEN, "No data for you!")

    def put(self, req, resp, name):
        # Raising without reading payload wil fail with EPIPE on the
        # client side. If the client is careful, it will get error 403.
        raise http.Error(http.FORBIDDEN, "No data for you!")


class KeepConnection(object):

    def put(self, req, resp):
        # Fail after reading the entire request payload, so the server
        # should keep the connection open.
        req.read()
        raise http.Error(http.FORBIDDEN, "No data for you!")


class PartialResponse(object):

    def get(self, req, resp):
        # Fail after sending the first part of the response. The
        # connection shold be closed.
        resp.headers["content-length"] = 1000
        resp.write(b"Starting response...")
        raise http.Error(http.INTERNAL_SERVER_ERROR, "No more data for you!")


@pytest.fixture(scope="module")
def server():
    server = http.Server(("", 0), http.Connection)
    log.info("Server listening on port %d", server.server_port)

    server.app = http.Router([
        (r"/demo/(.*)", Demo()),
        (r"/echo/(.*)", Echo()),
        (r"/request-info/(.*)", RequestInfo()),
        (r"/context/(.*)", Context()),
        (r"/close-context/(.*)", CloseContext()),
        (r"/server-error/(.*)", ServerError()),
        (r"/client-error/(.*)", ClientError()),
        (r"/keep-connection/", KeepConnection()),
        (r"/partial-response/", PartialResponse()),
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
        assert r.status == http.OK
        assert r.read() == b"name\n"


def test_demo_max_request_length(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # GET /demo/xxxxxxxxxx... HTTP/1.1\r\n
        name = "x" * 4075
        con.request("GET", "/demo/" + name)
        r = con.getresponse()
        assert r.status == http.OK
        assert r.read().decode("ascii") == name + "\n"


def test_demo_request_length_too_long(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # GET /demo/xxxxxxxxxx... HTTP/1.1\r\n
        con.request("GET", "/demo/" + "x" * 4076)
        r = con.getresponse()
        assert r.status == http.REQUEST_URI_TOO_LARGE
        r.read()


def test_demo_get_empty(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/demo/")
        r = con.getresponse()
        assert r.status == http.OK
        assert r.read() == b"\n"


def test_demo_delete(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("DELETE", "/demo/name")
        r = con.getresponse()
        assert r.status == http.NO_CONTENT
        assert r.read() == b""


def test_demo_options(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("OPTIONS", "/demo/name")
        r = con.getresponse()
        assert r.status == http.OK
        assert r.getheader("allow") == "GET,DELETE,OPTIONS"
        assert r.read() == b""


@pytest.mark.parametrize("data", [b"it works!", b""])
def test_echo(server, data):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("PUT", "/echo/test", body=data)
        r = con.getresponse()
        assert r.status == http.OK
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
        assert r.status == http.OK
        assert r.read() == data


def test_request_info_get(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/request-info/arg")
        r = con.getresponse()
        assert r.status == http.OK
        info = json.loads(r.read())

    assert info["method"] == "GET"
    assert info["uri"] == "/request-info/arg"
    assert info["path"] == "/request-info/arg"
    assert info["arg"] == "arg"
    assert info["query"] == {}
    assert info["version"] == "HTTP/1.1"
    assert info["content_length"] is None
    assert info["content"] is None
    assert info["headers"]["host"] == "localhost:%d" % server.server_port
    assert info["headers"]["accept-encoding"] == "identity"
    assert info["client_addr"] == "127.0.0.1"


def test_request_info_put(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        content = "it works!"
        con.request("PUT", "/request-info/arg", body=content.encode("utf-8"))
        r = con.getresponse()
        assert r.status == http.OK
        info = json.loads(r.read())

    assert info["method"] == "PUT"
    assert info["uri"] == "/request-info/arg"
    assert info["path"] == "/request-info/arg"
    assert info["arg"] == "arg"
    assert info["query"] == {}
    assert info["version"] == "HTTP/1.1"
    assert info["content_length"] == len(content)
    assert info["content"] == content
    assert info["headers"]["accept-encoding"] == "identity"
    assert info["headers"]["content-length"] == str(len(content))


@pytest.mark.parametrize("content_length", ["not an int", "-1"])
def test_request_invalid_content_length(server, content_length):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request(
            "GET",
            "/request-info/",
            headers={"content-length": content_length})
        r = con.getresponse()
        assert r.status == http.BAD_REQUEST


@pytest.mark.parametrize("uri,path,arg", [
    ("/request-info/%d7%90", u"/request-info/\u05d0", u"\u05d0"),
    ("/request-info%2farg", u"/request-info/arg", u"arg"),
])
def test_request_info_uri(server, uri, path, arg):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", uri)
        r = con.getresponse()
        assert r.status == http.OK
        info = json.loads(r.read())

    assert info["uri"] == uri
    assert info["path"] == path
    assert info["arg"] == arg


@pytest.mark.parametrize("query_string,parsed_query", [
    # Keep blank values.
    ("a", {"a": ""}),
    # Simple query.
    ("a=1&b=2", {"a": "1", "b": "2"}),
    # Multiple values, last wins.
    ("a=1&a=2", {"a": "2"}),
    # Multiple values, last empty.
    ("a=1&a=2&a", {"a": ""}),
    # Quoted keys and values.
    ("%61=%31", {"a": "1"}),
    # Decoded keys and values {Hebrew Letter Alef: Hebrew Leter Bet}
    # http://unicode.org/charts/PDF/U0590.pdf
    ("%d7%90=%d7%91", {u"\u05d0": u"\u05d1"}),
])
def test_request_info_query_string(server, query_string, parsed_query):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/request-info/?" + query_string)
        r = con.getresponse()
        assert r.status == http.OK
        info = json.loads(r.read())

    assert info["path"] == "/request-info/"
    assert info["query"] == parsed_query


def test_context(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # No context yet.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == http.NOT_FOUND
        r.read()

        # Set value for "this".
        con.request("PUT", "/context/this", body=b"value")
        r = con.getresponse()
        assert r.status == http.OK
        r.read()

        # Should have value now.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == http.OK
        assert r.read() == b"value"

        # Remove value for "this".
        con.request("DELETE", "/context/this")
        r = con.getresponse()
        assert r.status == http.NO_CONTENT
        r.read()

        # No context now.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == http.NOT_FOUND
        r.read()


def test_context_per_connection(server):
    con1 = http_client.HTTPConnection("localhost", server.server_port)
    con2 = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con1), closing(con2):
        # Set value for "this" in connection 1.
        con1.request("PUT", "/context/this", body=b"con1 value")
        r = con1.getresponse()
        assert r.status == http.OK
        r.read()

        # Connection 1 should have no value.
        con2.request("GET", "/context/this")
        r = con2.getresponse()
        assert r.status == http.NOT_FOUND
        r.read()

        # Set value for "this" in connection 2.
        con2.request("PUT", "/context/this", body=b"con2 value")
        r = con2.getresponse()
        assert r.status == http.OK
        r.read()

        # Connection 1 value did not change.
        con1.request("GET", "/context/this")
        r = con1.getresponse()
        assert r.status == http.OK
        assert r.read() == b"con1 value"


def test_context_deleted_on_close(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Set value for "this".
        con.request("PUT", "/context/this", body=b"con value")
        r = con.getresponse()
        assert r.status == http.OK
        r.read()

    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Should have no value.
        con.request("GET", "/context/this")
        r = con.getresponse()
        assert r.status == http.NOT_FOUND
        r.read()


def test_context_close(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Add a closable object to the connection context
        con.request("PUT", "/close-context/foo")
        r = con.getresponse()
        assert r.status == http.OK
        r.read()

    # Run server thread to detect the close.
    time.sleep(0.1)

    # Closing the connection should close the object.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/close-context/")
        r = con.getresponse()
        assert r.status == http.OK
        log = r.read().decode("utf-8")
        assert "foo was closed" in log


def test_context_close_multiple_objects(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Add a closable object to the connection context
        con.request("PUT", "/close-context/foo")
        r = con.getresponse()
        assert r.status == http.OK
        r.read()

        # Add another
        con.request("PUT", "/close-context/bar")
        r = con.getresponse()
        assert r.status == http.OK
        r.read()

    # Run server thread to detect the close.
    time.sleep(0.1)

    # Closing the connection should close both objects.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/close-context/")
        r = con.getresponse()
        assert r.status == http.OK
        log = r.read().decode("utf-8")
        assert "foo was closed" in log
        assert "bar was closed" in log


def test_not_found(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/no/such/path")
        r = con.getresponse()
        assert r.status == http.NOT_FOUND


def test_method_not_allowed(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("POST", "/demo/name")
        r = con.getresponse()
        assert r.status == http.METHOD_NOT_ALLOWED


def test_invalid_method(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("FOO", "/demo/name")
        r = con.getresponse()
        assert r.status == http.METHOD_NOT_ALLOWED


def test_client_error_get(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/client-error/")
        r = con.getresponse()
        assert r.status == http.FORBIDDEN


def test_client_error_put(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        try:
            con.request("PUT", "/client-error/", body=b"x" * 1024**2)
        except socket.error as e:
            if e.args[0] not in (errno.EPIPE, errno.ESHUTDOWN):
                raise
        r = con.getresponse()
        assert r.status == http.FORBIDDEN


def test_internal_error_get(server):
    # Internal error should not expose secret data in client response.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/server-error/")
        r = con.getresponse()
        assert r.status == http.INTERNAL_SERVER_ERROR
        assert "secret" not in r.read().decode("utf-8")


def test_internal_error_put(server):
    # Internal error should not expose secret data in client response.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        try:
            con.request("PUT", "/server-error/", body=b"x" * 1024**2)
        except socket.error as e:
            if e.args[0] not in (errno.EPIPE, errno.ESHUTDOWN):
                raise
        r = con.getresponse()
        assert r.status == http.INTERNAL_SERVER_ERROR
        assert "secret" not in r.read().decode("utf-8")


@pytest.mark.parametrize("data", [None, b"", b"read me"])
def test_keep_connection_on_error(server, data):
    # When a request does not have a payload, the server can keep the
    # connection open after and error.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Disabling auto_open so we can test if a connection was closed.
        con.auto_open = False
        con.connect()

        # Send couple of requests - all should fail, without closing the
        # connection.
        for i in range(3):
            con.request("PUT", "/keep-connection/", body=data)
            r = con.getresponse()
            r.read()
            assert r.status == http.FORBIDDEN


def test_close_connection_on_error(server):
    # When payload was not read completely, the server must close the
    # connection.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Disabling auto_open so we can test if a connection was closed.
        con.auto_open = False
        con.connect()

        # Send a request - it should fail without reading the request,
        # so the server will close the connection.
        con.request("PUT", "/client-error/", body=b"read me")
        r = con.getresponse()
        r.read()
        assert r.status == http.FORBIDDEN

        # Try to send another request. This will fail since we disabled
        # auto_open.  Fails in request() or in getresponse(), probably
        # depends on timing.
        with pytest.raises(
                (http_client.NotConnected, http_client.BadStatusLine)):
            con.request("GET", "/client-error/")
            con.getresponse()


def test_close_connection_on_partial_response(server):
    # When the response was not sent completely, the server must close
    # the connection.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Disabling auto_open so we can test if a connection was closed.
        con.auto_open = False
        con.connect()

        # Send a request - it should failed because the server
        # closed the connection before sending the entire response.
        con.request("GET", "/partial-response/")
        r = con.getresponse()
        assert r.status == http.OK
        with pytest.raises(http_client.IncompleteRead):
            r.read()

        # Try to send another request. This will fail since we disabled
        # auto_open.  Fails in request() or in getresponse(), probably
        # depends on timing.
        with pytest.raises(
                (http_client.NotConnected, http_client.BadStatusLine)):
            con.request("GET", "/client-error/")
            con.getresponse()
