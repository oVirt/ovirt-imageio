# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import logging
import re
import socket

import six
from six.moves import BaseHTTPServer
from six.moves import socketserver
from six.moves import urllib

from . import util

log = logging.getLogger("http")


# Taken from asyncore.py. Treat these as expected error when reading or writing
# to client connection.
_DISCONNECTED = frozenset((
    errno.ECONNRESET,
    errno.ENOTCONN,
    errno.ESHUTDOWN,
    errno.ECONNABORTED,
    errno.EPIPE
))


class Error(Exception):

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.message)


class Server(socketserver.ThreadingMixIn,
             BaseHTTPServer.HTTPServer):
    """
    Threaded HTTP server.
    """
    daemon_threads = True

    # A callable called for every request.
    app = None

    # Clock used to profile connections. The default NullClock does not
    # profile anything to minimize overhead. Set to util.Clock to enable
    # profiling.
    clock_class = util.NullClock


class Connection(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    HTTP server connection.

    The server create a new instance for every connection, and then
    process all requests on this connection using the server.app
    callable.
    """

    protocol_version = "HTTP/1.1"

    # Avoids possible delays when sending very small response.
    disable_nagle_algorithm = True

    # The maximum length of the request line:
    # https://tools.ietf.org/html/rfc2616#section-5.1. This limit the size of
    # the request URI. The spec does not define this length, and common
    # browsers support up to 65536 bytes. For imageio purposes, we don't need
    # to support long URIs, so we use small value.
    max_request_line = 4096

    def setup(self):
        log.info("OPEN client=%s", self.address_string())
        BaseHTTPServer.BaseHTTPRequestHandler.setup(self)
        # Per connection context, used by application to cache state.
        self.context = Context()
        self.clock = self.server.clock_class()
        self.clock.start("connection")

    def finish(self):
        self.clock.stop("connection")
        log.info("CLOSE client=%s %s", self.address_string(), self.clock)
        self.context.close()
        del self.context
        try:
            BaseHTTPServer.BaseHTTPRequestHandler.finish(self)
        except socket.error as e:
            if e.args[0] not in _DISCONNECTED:
                raise
            log.debug("Client disconnected client=%s", self.address_string())

    def handle_one_request(self):
        """
        Override to dispatch requests to server.app and improve error
        handling and logging.

        See the original version here:
        https://github.com/python/cpython/blob/2.7/Lib/BaseHTTPServer.py
        https://github.com/python/cpython/blob/master/Lib/http/server.py
        """
        try:
            self.raw_requestline = self.rfile.readline(
                self.max_request_line + 1)
            if len(self.raw_requestline) > self.max_request_line:
                log.warning("Request line too long: %d > %d, closing "
                            "connection",
                            len(self.raw_requestline), self.max_request_line)
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(414)
                return

            if not self.raw_requestline:
                log.debug("Empty request line, client disconnected")
                self.close_connection = 1
                return

            if not self.parse_request():
                return

            self.server.app(Request(self), Response(self))
            self.wfile.flush()

        except socket.timeout as e:
            log.warning("Timeout reading or writing to socket: %s", e)
            self.close_connection = 1
        except socket.error as e:
            if e.args[0] not in _DISCONNECTED:
                raise
            log.debug("Client disconnected: %s", e)
            self.close_connection = 1

    def address_string(self):
        """
        Override to avoid slow and unneeded name lookup.
        """
        return self.client_address[0]

    def log_request(self, msg):
        """
        Override to disable server logs.
        """


class Request(object):

    def __init__(self, con):
        self._con = con
        if "?" in con.path:
            self._path, self._query_string = con.path.split("?", 1)
        else:
            self._path, self._query_string = con.path, ""
        self._query = None  # Parsed lazily.

    @property
    def context(self):
        """
        Return connection context dict. Can be used to cache per
        connection that can be used by future request on this
        connection.

        Objects added to this dict are discarded when the connection is
        closed. If the objects implement "close", they are also closed.
        """
        return self._con.context

    @property
    def clock(self):
        """
        Return connection clock.

        Request handlers may use this clock to time operations. Timed
        operations are logged when closing a connection.
        """
        return self._con.clock

    @property
    def headers(self):
        return self._con.headers

    @property
    def version(self):
        return self._con.request_version

    @property
    def method(self):
        return self._con.command

    @property
    def path(self):
        return self._path

    @property
    def query(self):
        """
        Return parsed query string dict.

        If key appears multiple times, the last key=value pair wins:

            key=1&key=2&key -> {"key": ""}

        Both keys and values are decoded to unicode on python 2 and str on
        python 3. Value that cannot be decoded to utf-8 are dropped silently.
        """
        if self._query is None:
            self._query = dict(parse_qsl(
                self._query_string, keep_blank_values=True))
        return self._query

    @property
    def client_addr(self):
        return self._con.address_string()

    def read(self, n=None):
        return self._con.rfile.read(n)


class Response(object):

    def __init__(self, con):
        self._con = con
        self.status_code = 200
        self.headers = {}
        self._started = False

    @property
    def started(self):
        """
        Return True if the response started.
        """
        return self._started

    def send_info(self, status_code, message=None):
        """
        Send informational status response (1xx) to the client, before
        sending the actual response.

        This can be called multiple times before sending the actual
        response.
        """
        if self._started:
            raise AssertionError("Response already sent")

        msg = self._con.responses[status_code][0].encode("ascii")
        self._con.wfile.write(b"HTTP/1.1 %d %s\r\n\r\n" % (status_code, msg))
        self._con.wfile.flush()

    def write(self, data):
        """
        Write data to the response body.

        The first call to write will send the HTTP header.
        """
        self._write(data)

    def _write(self, data):
        """
        The initial write to the client, sending the status line and
        headers.

        This method is replaced by the underlying output file write
        method after the first call.
        """
        self._started = True
        self._con.send_response(self.status_code)
        for name, value in six.iteritems(self.headers):
            self._con.send_header(name, value)
        self._con.end_headers()
        self._con.wfile.write(data)
        self._write = self._con.wfile.write


class Context(dict):
    """
    A dict with close interface, closing all closable values.
    """

    def close(self):
        for v in self.values():
            if hasattr(v, "close"):
                try:
                    v.close()
                except Exception:
                    log.exception("Error closing %s", v)


class Router(object):
    """
    Route requests to registered requests handlers.
    """

    ALLOWED_METHODS = frozenset(
        ['GET', 'PUT', 'PATCH', 'POST', 'DELETE', 'OPTIONS', 'HEAD'])

    def __init__(self, routes):
        self._routes = [(re.compile(pattern), handler)
                        for pattern, handler in routes]

    def __call__(self, req, resp):
        with req.clock.run("dispatch"):
            try:
                self.dispatch(req, resp)
                # If request has no content, we need to invoke write()
                # to send the headers. Doing it here simplify client
                # code that want to return the default 200 OK response.
                if not resp.started:
                    resp.write(b"")
            except socket.error as e:
                # TODO: Verify that error is in the connection socket.
                if e.args[0] in _DISCONNECTED:
                    # We cannot send a response.
                    log.warning("Client disconnected: %s", e)
                else:
                    log.exception("Server error")
                    # Likely to fail, but lets try anyway.
                    self.send_error(resp, 500, "Internal server error")
            except Exception as e:
                if not isinstance(e, Error):
                    # Don't expose internal errors to client.
                    e = Error(500, "Internal server error")
                if e.code >= 500:
                    log.exception("Server error")
                elif e.code >= 400:
                    log.warning("Client error: %s", e)
                self.send_error(resp, e.code, str(e))

    def dispatch(self, req, resp):
        if req.method not in self.ALLOWED_METHODS:
            raise Error(405, "Invalid method {!r}".format(req.method))

        path = req.path
        for route, handler in self._routes:
            match = route.match(path)
            if match:
                try:
                    method = getattr(handler, req.method.lower())
                except AttributeError:
                    raise Error(405, "Method {!r} not defined for {!r}"
                                .format(req.method, path))
                return method(req, resp, *match.groups())

        raise Error(404, "No handler for {!r}".format(path))

    def send_error(self, resp, code, message):
        # TODO: return json errors.
        body = message.encode("utf-8")
        resp.status_code = code
        resp.headers["content-length"] = len(body)
        resp.write(body)


# Compatibility hacks

if six.PY2:
    def parse_qsl(qs, keep_blank_values=False, strict_parsing=False):
        for k, v in urllib.parse.parse_qsl(
                qs,
                keep_blank_values=keep_blank_values,
                strict_parsing=strict_parsing):
            try:
                yield k.decode("utf-8"), v.decode("utf-8")
            except UnicodeDecodeError:
                if strict_parsing:
                    raise
else:
    parse_qsl = urllib.parse.parse_qsl
