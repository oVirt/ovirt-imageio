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

# Sentinel for lazy initialization, ensuring that we initialize only once.
_UNKNOWN = object()


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
        self._query = _UNKNOWN
        self._content_length = _UNKNOWN
        self._length = _UNKNOWN

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
        if self._query is _UNKNOWN:
            self._query = dict(parse_qsl(
                self._query_string, keep_blank_values=True))
        return self._query

    @property
    def content_length(self):
        """
        Return parsed Content-Length header value, or None if the header is
        missing.

        Raises:
            http.Error if Content-Length is not a positive integer.
        """
        if self._content_length is _UNKNOWN:
            value = self._con.headers.get("content-length")
            if value is not None:
                # Requiring valid value here avoid this error handling code
                # from application code.
                # Note: set to None on errors to avoid failures when handling
                # the error.
                try:
                    value = int(value)
                except ValueError:
                    self._content_length = None
                    raise Error(
                        400, "Invalid Content-Length: {!r}".format(value))
                if value < 0:
                    self._content_length = None
                    raise Error(
                        400, "Negative Content-Length: {!r}".format(value))
            self._content_length = value
        return self._content_length

    @property
    def length(self):
        """
        Return the number of unread bytes left in the request body, or None if
        the request does not have valid content length.
        """
        if self._length is _UNKNOWN:
            self._length = self.content_length
        return self._length

    @property
    def client_addr(self):
        return self._con.address_string()

    def read(self, n=None):
        # TODO: support chunked encoding.
        if not self.length:
            return b""

        if n is None or n > self._length:
            n = self._length

        data = self._con.rfile.read(n)
        self._length -= len(data)

        return data


class Response(object):

    def __init__(self, con):
        self._con = con
        self.status_code = 200
        self.headers = Headers()
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

        NOTE: This method is replaced with the underlying file write method on
        the first call. Do not try to cache this method!
        """
        self._started = True
        header = self._format_header()
        self._update_connection()

        # TODO: Check if saving one syscall for small payloads (e.g. < 1460) by
        # merging header and data improves performance. Note that data may be a
        # buffer or memoryview object wrapping a mmap object.
        self._con.wfile.write(header)
        self._con.wfile.write(data)
        self._con.wfile.flush()

        # This avoids name lookup on the next calls to write.
        self.write = self._con.wfile.write

    def _format_header(self):
        """
        Format HTTP header using temporary buffer, avoiding one syscall per
        line in python 2.7.
        """
        if self.status_code in self._con.responses:
            msg = self._con.responses[self.status_code][0].encode("latin1")
        else:
            msg = b""

        header = io.BytesIO()

        # Write response line.
        header.write(b"%s %d %s\r\n" % (
                     self._con.protocol_version.encode("latin1"),
                     self.status_code,
                     msg))

        # Write default headers.
        header.write(b"server: %s\r\n" %
                     self._con.version_string().encode("latin1"))
        header.write(b"date: %s\r\n" %
                     self._con.date_time_string().encode("latin1"))

        # Write user headers.
        for name, value in six.iteritems(self.headers):
            # Encoding entire line to allow using integer value, for example
            # content-length.
            header.write(("%s: %s\r\n" % (name, value)).encode("latin1"))

        # End header.
        header.write(b"\r\n")

        return header.getvalue()

    def _update_connection(self):
        """
        Update connection based on response headers.
        """
        connection = self.headers.get("connection")
        if connection:
            if connection == "close":
                self._con.close_connection = 1
            elif connection == "keep-alive":
                self._con.close_connection = 0


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


class Headers(dict):
    """
    Dictionarry optimized for keeping HTTP headers.
    """

    def __setitem__(self, name, value):
        """
        Override to enforce lowercase keys and lowercase values for certain
        keys. This make it easier to get and use values from response headers
        when processing the response.
        """
        name = name.lower()
        if name == "connection":
            value = value.lower()
        dict.__setitem__(self, name, value)


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
                    if not resp.started:
                        self.send_error(resp, 500, "Internal server error")
            except Exception as e:
                if not isinstance(e, Error):
                    # Don't expose internal errors to client.
                    e = Error(500, "Internal server error")
                if e.code >= 500:
                    log.exception("Server error")
                elif e.code >= 400:
                    log.warning("Client error: %s", e)
                if req.length is not None and req.length > 0:
                    log.debug("Request failed before reading entire content, "
                              "closing connection")
                    resp.headers["connection"] = "close"
                if not resp.started:
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
