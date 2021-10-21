# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import http.client as http_client
import io
import json
import logging
import os
import socket
import time

from contextlib import closing
from contextlib import contextmanager

import pytest

from ovirt_imageio._internal import http
from ovirt_imageio._internal import util
from ovirt_imageio._internal import version

log = logging.getLogger("test")

ERROR_CONTENT_TYPE = "text/plain; charset=UTF-8"


class Demo:

    def get(self, req, resp, name):
        body = b"%s\n" % name.encode("utf-8")
        resp.headers["content-length"] = len(body)
        # Test handling for UTF-8 values (Hebrew Alef). This usage is not
        # correct according to HTTP RFCs, but it seems to be supported by the
        # browsers we tested.  See https://tools.ietf.org/html/rfc5987 for a
        # more correct way to do this.
        cd = b"attachment; filename=\xd7\x90".decode("utf-8")
        resp.headers["content-disposition"] = cd
        resp.write(body)

    def delete(self, req, resp, name):
        resp.status_code = http.NO_CONTENT

    def options(self, req, resp, name):
        resp.headers["allow"] = "GET,DELETE,OPTIONS"


class EchoRead:

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


class EchoReadinto:

    def put(self, req, resp, ticket):
        if req.headers.get("expect") == "100-continue":
            resp.send_info(http.CONTINUE)

        count = req.content_length
        resp.headers["content-length"] = count

        buf = bytearray(1024**2)
        with memoryview(buf) as view:
            while count:
                n = req.readinto(buf)
                if not n:
                    raise http.Error(http.BAD_REQUEST, "Client disconnected")
                resp.write(view[:n])
                count -= n


class JSON:

    def put(self, req, resp):
        msg = json.loads(req.read())
        resp.send_json(msg)


class RangeDemo:
    """
    Demonstrate using Range and Content-Range headers.
    """

    def __init__(self):
        self.file = io.BytesIO()

    def get(self, req, resp):
        complete = self.file.seek(0, os.SEEK_END)
        offset = 0
        size = complete

        # Handle range request.
        if req.range:
            offset = req.range.first
            if offset < 0:
                offset += complete
                size = complete - offset
            elif req.range.last is not None:
                size = req.range.last - offset + 1
            else:
                size = complete - offset

        if offset + size > complete:
            raise http.Error(
                http.REQUESTED_RANGE_NOT_SATISFIABLE,
                "Requested {} bytes, available {} bytes"
                .format(size, complete - offset),
                content_range="bytes */{}".format(complete - offset))

        resp.headers["content-length"] = size

        if req.range:
            resp.status_code = http.PARTIAL_CONTENT
            resp.headers["content-range"] = "bytes %d-%d/%d" % (
                offset, offset + size - 1, complete)

        self.file.seek(offset)
        resp.write(self.file.read(size))

    def put(self, req, resp):
        offset = req.content_range.first if req.content_range else 0
        self.file.seek(offset)
        self.file.write(req.read())
        if req.length != 0:
            raise http.Error(http.BAD_REQUEST, "Unexpected EOF")


class RequestInfo:

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
        for k, v in req.query.items():
            assert type(k) == str
            assert type(v) == str

        # Simple values
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

        # Complex values
        if req.range:
            info["range"] = {"first": req.range.first,
                             "last": req.range.last}
        else:
            info["range"] = req.range
        if req.content_range:
            info["content_range"] = {"first": req.content_range.first,
                                     "last": req.content_range.last,
                                     "complete": req.content_range.complete}
        else:
            info["content_range"] = req.content_range

        resp.send_json(info)


class Context:
    """
    Keep per-connection state example.
    """

    def put(self, req, resp, name):
        value = req.read()
        req.context[name] = value

    def get(self, req, resp, name):
        if name not in req.context:
            raise http.Error(http.NOT_FOUND, "No such name {!r}".format(name))
        value = req.context[name]
        resp.headers["content-length"] = len(value)
        resp.write(value)

    def delete(self, req, resp, name):
        req.context.pop(name, None)
        resp.status_code = http.NO_CONTENT


class Closeable:

    def __init__(self, name, log):
        self.name = name
        self.log = log

    def close(self):
        self.log.write("{} was closed\n".format(self.name))
        # For checking that all objects are closed when a connection is closed.
        raise RuntimeError("Error closing {!r}".format(self.name))


class CloseContext:
    """
    Example for closing objects when connection is closed.
    """

    def __init__(self):
        self.log = io.StringIO()

    def put(self, req, resp, name):
        req.context[name] = Closeable(name, self.log)

    def get(self, req, resp, *args):
        value = self.log.getvalue().encode("utf-8")
        self.log = io.StringIO()
        resp.headers["content-length"] = len(value)
        resp.write(value)


class ServerError:

    def get(self, req, resp, name):
        raise RuntimeError("secret data")

    def put(self, req, resp, name):
        # Raising without reading payload wil fail with EPIPE on the
        # client side. If the client is careful, it will get error 500.
        raise RuntimeError("secret data")


class ServerSocketError:

    def get(self, req, resp, name):
        # Fake a fatal socket error that is not related to the HTTP connection.
        # An example error is a backend failing to connect to a remote server.
        # We expect to get INTERNAL_SERVER_ERROR on the client side.
        # Until 1.5.2, this error was wrongly handled as an error on the HTTP
        # connection, and we closed the connection silently instead of
        # returning an error to the client.
        raise socket.error(errno.ECONNRESET, "fake socket error")

    put = get


class ClientError:

    def get(self, req, resp, name):
        raise http.Error(http.FORBIDDEN, "No data for you!")

    def put(self, req, resp, name):
        # Raising without reading payload wil fail with EPIPE on the
        # client side. If the client is careful, it will get error 403.
        raise http.Error(http.FORBIDDEN, "No data for you!")


class KeepConnection:

    def put(self, req, resp):
        # Fail after reading the entire request payload, so the server
        # should keep the connection open.
        req.read()
        raise http.Error(http.FORBIDDEN, "No data for you!")


class PartialResponse:

    def get(self, req, resp):
        # Fail after sending the first part of the response. The
        # connection shold be closed.
        resp.headers["content-length"] = 1000
        resp.write(b"Starting response...")
        raise http.Error(http.INTERNAL_SERVER_ERROR, "No more data for you!")


@contextmanager
def demo_server(address, prefer_ipv4=False):
    server = http.Server(
        (address, 0), http.Connection, prefer_ipv4=prefer_ipv4)
    log.info("Server listening on %r", server.server_address)
    server.app = http.Router([(r"/demo/(.*)", Demo())])

    t = util.start_thread(
        server.serve_forever,
        kwargs={"poll_interval": 0.1})
    try:
        yield server
    finally:
        server.shutdown()
        t.join()


@pytest.fixture(scope="module")
def server():
    server = http.Server(("127.0.0.1", 0), http.Connection)
    log.info("Server listening on %r", server.server_address)

    server.app = http.Router([
        (r"/demo/(.*)", Demo()),
        (r"/echo-read/(.*)", EchoRead()),
        (r"/echo-readinto/(.*)", EchoReadinto()),
        (r"/json/", JSON()),
        (r"/range-demo/", RangeDemo()),
        (r"/request-info/(.*)", RequestInfo()),
        (r"/context/(.*)", Context()),
        (r"/close-context/(.*)", CloseContext()),
        (r"/server-error/(.*)", ServerError()),
        (r"/server-socket-error/(.*)", ServerSocketError()),
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


def test_demo_headers(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/demo/name")
        r = con.getresponse()
        r.read()
        assert r.getheader("Server") == "imageio/" + version.string


def test_demo_get(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/demo/name")
        r = con.getresponse()
        assert r.status == http.OK
        assert r.read() == b"name\n"


def test_demo_get_utf8_headers(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/demo/name")
        r = con.getresponse()
        r.read()
        assert r.status == http.OK
        cd = r.getheader("content-disposition")
        # HTTP permit only ASCII for headers content, so python 3 decode
        # headers using latin1. This fixes the bad decoding.
        cd = cd.encode("latin1")
        cd = cd.decode("utf-8")
        assert cd == b"attachment; filename=\xd7\x90".decode("utf-8")


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
def test_echo_read(server, data):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("PUT", "/echo-read/test", body=data)
        r = con.getresponse()
        assert r.status == http.OK
        assert r.read() == data


@pytest.mark.parametrize("data", [b"it works!", b""])
def test_echo_readinto(server, data):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("PUT", "/echo-readinto/test", body=data)
        r = con.getresponse()
        assert r.status == http.OK
        assert r.read() == data


def test_echo_100_continue(server):
    data = b"it works!"
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request(
            "PUT",
            "/echo-read/test",
            body=data,
            headers={"expect": "100-continue"})
        r = con.getresponse()
        assert r.status == http.OK
        assert r.read() == data


def test_json(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        send = {"Hebrew Alef": "\u05d0"}
        data = json.dumps(send).encode("utf-8") + b"\n"
        con.request("PUT", "/json/", body=data)
        r = con.getresponse()
        data = r.read()
        assert r.status == http.OK
        assert r.getheader("content-type") == "application/json"
        assert int(r.getheader("content-length")) == len(data)
        assert data.endswith(b"\n")
        recv = json.loads(data)
        assert send == recv


def test_range_demo(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        # Get complete resource, should return 0 bytes.
        con.request("GET", "/range-demo/")
        r = con.getresponse()
        body = r.read()
        assert r.status == http.OK
        assert body == b""

        # Put some data.
        con.request("PUT", "/range-demo/", body=b"it works!")
        r = con.getresponse()
        r.read()
        assert r.status == http.OK

        # Get part of the resource.
        con.request("GET", "/range-demo/", headers={"range": "bytes=3-"})
        r = con.getresponse()
        body = r.read()
        assert r.status == http.PARTIAL_CONTENT
        assert body == b"works!"

        # Replace part of the resource.
        con.request("PUT", "/range-demo/",
                    body=b"really works!",
                    headers={"content-range": "bytes 3-*/*"})
        r = con.getresponse()
        r.read()
        assert r.status == http.OK

        # Get last bytes of the resource.
        con.request("GET", "/range-demo/", headers={"range": "bytes=-13"})
        r = con.getresponse()
        body = r.read()
        assert r.status == http.PARTIAL_CONTENT
        assert body == b"really works!"

        # Get invalid range after the last byte.
        con.request("GET", "/range-demo/", headers={"range": "bytes=0-100"})
        r = con.getresponse()
        r.read()
        assert r.status == http.REQUESTED_RANGE_NOT_SATISFIABLE
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE
        assert r.getheader("content-range") == "bytes */16"


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
    assert info["range"] is None


def test_request_info_get_range(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/request-info/arg",
                    headers={"range": "bytes=0-99"})
        r = con.getresponse()
        body = r.read()
        assert r.status == http.OK

    info = json.loads(body)
    assert info["range"] == {"first": 0, "last": 99}


def test_request_info_get_range_last_specified(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/request-info/arg",
                    headers={"range": "bytes=0-"})
        r = con.getresponse()
        body = r.read()
        assert r.status == http.OK

    info = json.loads(body)
    assert info["range"] == {"first": 0, "last": None}


def test_request_info_get_unsatisfiable_range(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/request-info/arg",
                    headers={"range": "bytes=invalid-99"})
        r = con.getresponse()
        r.read()
        assert r.status == http.REQUESTED_RANGE_NOT_SATISFIABLE
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


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
    assert info["content_range"] is None


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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


def test_request_info_put_content_range(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        content = "it works!"
        con.request("PUT", "/request-info/arg",
                    body=content.encode("utf-8"),
                    headers={"content-range": "bytes 0-8/100"})
        r = con.getresponse()
        body = r.read()
        assert r.status == http.OK

    info = json.loads(body)
    assert info["content_range"] == {"first": 0, "last": 8, "complete": 100}


def test_request_info_put_content_range_invalid(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        content = "it works!"
        con.request("PUT", "/request-info/arg",
                    body=content.encode("utf-8"),
                    headers={"content-range": "bytes 0-invalid/*"})
        r = con.getresponse()
        r.read()
        assert r.status == http.BAD_REQUEST
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


@pytest.mark.parametrize("uri,path,arg", [
    ("/request-info/%d7%90", "/request-info/\u05d0", "\u05d0"),
    ("/request-info%2farg", "/request-info/arg", "arg"),
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
    ("%d7%90=%d7%91", {"\u05d0": "\u05d1"}),
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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE
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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE
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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE
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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE
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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


def test_method_not_allowed(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("POST", "/demo/name")
        r = con.getresponse()
        assert r.status == http.METHOD_NOT_ALLOWED
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


def test_invalid_method(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("FOO", "/demo/name")
        r = con.getresponse()
        assert r.status == http.METHOD_NOT_ALLOWED
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


def test_client_error_get(server):
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/client-error/")
        r = con.getresponse()
        assert r.status == http.FORBIDDEN
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


def test_internal_error_get(server):
    # Internal error should not expose secret data in client response.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/server-error/")
        r = con.getresponse()
        assert r.status == http.INTERNAL_SERVER_ERROR
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE
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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE
        assert "secret" not in r.read().decode("utf-8")


def test_server_socket_error_get(server):
    # Socket error on server side should report as INTERNAL_SERVER_ERROR.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        con.request("GET", "/server-socket-error/")
        r = con.getresponse()
        assert r.status == http.INTERNAL_SERVER_ERROR
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


def test_server_socket_error_put(server):
    # Socket error on server side should report as INTERNAL_SERVER_ERROR.
    con = http_client.HTTPConnection("localhost", server.server_port)
    with closing(con):
        try:
            con.request("PUT", "/server-socket-error/", body=b"x" * 1024**2)
        except socket.error as e:
            if e.args[0] not in (errno.EPIPE, errno.ESHUTDOWN):
                raise
        r = con.getresponse()
        assert r.status == http.INTERNAL_SERVER_ERROR
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE


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
            assert r.getheader("content-type") == ERROR_CONTENT_TYPE


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
        assert r.getheader("content-type") == ERROR_CONTENT_TYPE

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


@pytest.mark.parametrize("header,first,last", [
    # Both first and last
    ("bytes=0-99", 0, 99),
    # One byte range.
    ("bytes=42-42", 42, 42),
    # From 99 to end.
    ("bytes=99-", 99, None),
    # Last 99 byts
    ("bytes=-99", -99, None),
])
def test_range_parse(header, first, last):
    r = http.Range.parse(header)
    assert r.first == first
    assert r.last is last


@pytest.mark.parametrize("header", [
    # Missing bytes
    "cats=0-99",
    # Wrong case
    "BYTES=0-99",
    # Extra spaces
    "bytes =0-99",
    "bytes= 0-99",
    "bytes=0 -99",
    "bytes=0- 99",
    "bytes=0-99 ",
    # Missing =
    "bytes 0-99",
    # Missing -
    "bytes=99",
    # first > last
    "bytes=99-98",
    # negative first
    "bytes=-42-",
    # first and negative last (conflict)
    "bytes=42--99",
    # ultiple ranges not supported yet.
    "bytes=0-499,500-599",
])
def test_range_parse_not_satisfiable(header):
    with pytest.raises(http.Error) as e:
        http.Range.parse(header)
    assert e.value.code == http.REQUESTED_RANGE_NOT_SATISFIABLE


@pytest.mark.parametrize("header,first,last,complete", [
    # First 100 bytes of 200 bytes.
    ("bytes 0-99/200", 0, 99, 200),
    # First byte of 200 bytes.
    ("bytes 0-0/200", 0, 0, 200),
    # Last byte of 200 bytes.
    ("bytes 199-199/200", 199, 199, 200),
    # Last unspeficied
    ("bytes 100-*/200", 100, None, 200),
    # Complete unspecified
    ("bytes 100-199/*", 100, 199, None),
    # Last and complete unspecified
    ("bytes 100-*/*", 100, None, None),
])
def test_content_range_parse(header, first, last, complete):
    r = http.ContentRange.parse(header)
    assert r.first == first
    assert r.last is last
    assert r.complete is complete


@pytest.mark.parametrize("header", [
    # Unsupported unit
    "cats 0-99/200",
    # Missing element
    "bytes 0-99/",
    "bytes 0-99",
    "bytes -99/200",
    "bytes 99-/200",
    "bytes 99/200",
    "bytes /200",
    # Invalid numberic values
    "bytes invalid-99/200",
    "bytes 0-invalid/200",
    "bytes 0-99/invalid",
    # Unspecified first
    "bytes *-99/200",
    # first > last
    "bytes 99-98/200",
    # last >= complete
    "bytes 0-200/200",
    "bytes 0-201/200",
    # Unsatisfied range (valid, but unsupported)
    "bytes */200",
])
def test_content_range_parse_invalid(header):
    with pytest.raises(http.Error) as e:
        http.ContentRange.parse(header)
    assert e.value.code == http.BAD_REQUEST


@pytest.mark.parametrize("listen_address, expected_server_address", [
    ("localhost", "::1"),
    ("127.0.0.1", "127.0.0.1"),
    ("::1", "::1"),
])
def test_server_bind_loopback(listen_address, expected_server_address):
    with demo_server(listen_address) as server:
        assert server.server_address[0] == expected_server_address
        # TCPServer returns tuple of different length for IPv4 and IPv6. We
        # change server_address in Server.server_bind() to be always tuple
        # of hostname and port. Check it here, that it has always two elements.
        assert len(server.server_address) == 2

        con = http_client.HTTPConnection("localhost", server.server_port)
        with closing(con):
            con.request("GET", "/demo/name")
            con.getresponse()


def test_server_bind_ipv4():
    with demo_server("") as server:
        # Connecting via IPv4 address should work.
        con = http_client.HTTPConnection("127.0.0.1", server.server_port)
        with closing(con):
            con.request("GET", "/demo/name")
            con.getresponse()

        # Connecting via IPv6 address should fail.
        con = http_client.HTTPConnection("::1", server.server_port)
        with closing(con):
            with pytest.raises(ConnectionRefusedError):
                con.request("GET", "/demo/name")


@pytest.mark.parametrize("connect_address", [
    "127.0.0.1",
    "::1",
    "localhost",
    socket.gethostname(),
])
def test_server_bind_dual_stack(connect_address):
    with demo_server("::") as server:
        con = http_client.HTTPConnection(connect_address, server.server_port)
        with closing(con):
            con.request("GET", "/demo/name")
            con.getresponse()


@pytest.mark.parametrize(
    "address", [a[4][0] for a in http.find_addresses(socket.gethostname())])
def test_server_bind_addr_from_hostname(address):
    with demo_server(address) as server:
        con = http_client.HTTPConnection(address, server.server_port)
        with closing(con):
            con.request("GET", "/demo/name")
            con.getresponse()


@pytest.mark.parametrize("listen_address", [
    "localhost",
    "127.0.0.1",
])
def test_prefer_ipv4(listen_address):
    with demo_server(listen_address, prefer_ipv4=True) as server:
        assert server.server_address[0] == "127.0.0.1"

        con = http_client.HTTPConnection(listen_address, server.server_port)
        with closing(con):
            con.request("GET", "/demo/name")
            con.getresponse()
