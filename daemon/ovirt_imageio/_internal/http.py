# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import http.server
import io
import ipaddress
import itertools
import json
import logging
import re
import socket
import socketserver
import urllib

from . import errors
from . import stats
from . import version

log = logging.getLogger("http")

# Common HTTP status codes
# See https://tools.ietf.org/html/rfc2616#section-6.1.1
CONTINUE = 100
OK = 200
NO_CONTENT = 204
PARTIAL_CONTENT = 206
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
METHOD_NOT_ALLOWED = 405
NOT_ACCEPTABLE = 406
CONFLICT = 409
REQUEST_URI_TOO_LARGE = 414
REQUESTED_RANGE_NOT_SATISFIABLE = 416
INTERNAL_SERVER_ERROR = 500

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

    def __init__(self, code, message, content_range=None):
        self.code = code
        self.message = message
        self.content_range = content_range

    def __str__(self):
        return str(self.message)


class Server(socketserver.ThreadingMixIn,
             http.server.HTTPServer):
    """
    Threaded HTTP server.
    """
    daemon_threads = True

    # A callable called for every request.
    app = None

    # Clock used to profile connections. The default NullClock does not
    # profile anything to minimize overhead. Set to stats.Clock to enable
    # profiling.
    clock_class = stats.NullClock

    def __init__(self, server_address, RequestHandlerClass, prefer_ipv4=False):
        super().__init__(
            server_address, RequestHandlerClass, bind_and_activate=False)

        # Close old socket created in parent constructor.
        if self.socket:
            self.socket.close()

        try:
            self.create_socket(prefer_ipv4)
            self.server_bind()
            self.server_activate()
        except BaseException:
            self.server_close()
            raise

    def create_socket(self, prefer_ipv4=False):
        """
        Create socket with correct socket family.
        If prefer_ipv4 is set, and both ipv4 and ipv6 addresses are
        available, pick the first ipv4 address.
        """
        host, port = self.server_address

        addresses = list(find_addresses(host, port=port))

        # If IPv4 is preferred, sort addresses according to address family, so
        # that IPv4 addresses are before IPv6 addresses. Addresses is a list of
        # tuples, address family being the first item in the tuple.
        log.debug("Prefer IPv4: %s", prefer_ipv4)
        if prefer_ipv4:
            addresses.sort(key=lambda x: x[0] != socket.AF_INET)

        log.debug("Available network interfaces: %s", addresses)
        self.address_family = addresses[0][0]

        # Create new socket with correct address family.
        log.debug(
            "Creating server socket with family=%s and type=%s",
            self.address_family,
            self.socket_type)
        try:
            self.socket = socket.socket(self.address_family, self.socket_type)
        except OSError as e:
            raise errors.ServerStartupError(
                "Failed to create socket for address={!r} address family={} "
                "socket type={} prefer IPv4={}. Underlying error is {}".format(
                    host,
                    self.address_family,
                    self.socket_type,
                    prefer_ipv4,
                    e))

    def server_bind(self):
        """
        Override server_bind to make server_address uniform.
        """
        super().server_bind()

        # TCPServer.server_bind() overwrites server_address with
        # socket.getsockname(), which is a tuple of two values when bound to
        # IPv4 interface (e.g. ('0.0.0.0', 54322)) and tuple of four values
        # when bound to IPv6 interface (e.g. ('::', 54322, 0, 0)). To make
        # server address uniform, ensure it's always a tuple of two values -
        # hostname/IP address and port number.
        self.server_address = self.server_address[:2]


class Connection(http.server.BaseHTTPRequestHandler):
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

    # Number of second to wait for recv() or send(). When the timeout expires
    # we close the connection. This is important when working with clients that
    # keep the connection open after upload or download (e.g browsers).
    timeout = 60

    # For generating connection ids. Start from 1 to match the connection
    # thread name.
    _counter = itertools.count(1)

    def setup(self):
        self.id = next(self._counter)
        log.info("OPEN connection=%s client=%s",
                 self.id, self.address_string())
        super().setup()
        # Per connection context, used by application to cache state.
        self.context = Context()
        self.clock = self.server.clock_class()
        self.clock.start("connection")

    def finish(self):
        self.clock.stop("connection")
        self.context.close()
        del self.context
        log.info("CLOSE connection=%s client=%s %s",
                 self.id, self.address_string(), self.clock)
        try:
            super().finish()
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
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(REQUEST_URI_TOO_LARGE)
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
        Override to disable request logs from send_response().
        """

    def log_error(self, fmt, *args):
        """
        Override to log errors from send_error() to our log instead of
        stderr. Since these are always client errors, log a warning.
        """
        log.warning(fmt, *args)

    def connection_error(self):
        """
        Return the error number from the underlying socket, or 0 if the socket
        has no error.
        """
        return self.connection.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    def version_string(self):
        """
        Used in Server header.
        """
        return "imageio/" + version.string


class Request:

    def __init__(self, con):
        self._con = con
        self._uri = con.path
        if "?" in con.path:
            path, self._query_string = con.path.split("?", 1)
        else:
            path, self._query_string = con.path, ""
        self._path = urllib.parse.unquote(path)
        self._query = _UNKNOWN
        self._content_length = _UNKNOWN
        self._length = _UNKNOWN
        self._range = _UNKNOWN
        self._content_range = _UNKNOWN

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
    def connection_id(self):
        return self._con.id

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
    def uri(self):
        """
        Return raw request URI including path and query string. This is mostly
        useful for logging.
        """
        return self._uri

    @property
    def path(self):
        """
        Return unquoted path component from the request URI.
        """
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
            self._query = dict(urllib.parse.parse_qsl(
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
                    raise Error(BAD_REQUEST,
                                "Invalid Content-Length: {!r}".format(value))
                if value < 0:
                    self._content_length = None
                    raise Error(BAD_REQUEST,
                                "Negative Content-Length: {!r}".format(value))
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
    def range(self):
        if self._range is _UNKNOWN:
            value = self.headers.get("range")
            if value is not None:
                self._range = Range.parse(value)
            else:
                self._range = None
        return self._range

    @property
    def content_range(self):
        if self._content_range is _UNKNOWN:
            value = self.headers.get("content-range")
            if value is not None:
                self._content_range = ContentRange.parse(value)
            else:
                self._content_range = None
        return self._content_range

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

    def readinto(self, buf):
        # TODO: support chunked encoding.
        if not self.length:
            return 0

        length = min(len(buf), self._length)

        with memoryview(buf)[:length] as view:
            n = self._con.rfile.readinto(view)

        self._length -= n
        return n

    def connection_lost(self):
        """
        Return True if the underlying socket was disconnected.
        """
        return self._con.connection_error() in _DISCONNECTED


class Response:

    def __init__(self, con):
        self._con = con
        self.status_code = OK
        self.headers = Headers({"content-length": 0})
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

    def send_error(self, e):
        """
        Send an error response for error e.
        """
        if self._started:
            raise AssertionError("Response already sent")

        self.status_code = e.code

        # Adding newline makes it easier to debug from the command line.
        body = str(e).encode("utf-8") + b"\n"

        # Content-type header of the response is set to text/plain to mark
        # content as non-interpretable by the client and thus avoid false
        # positive from security analysis tools when we send requested URL
        # back as part of, say, 404 response
        self.headers["content-type"] = "text/plain; charset=UTF-8"

        self.headers["content-length"] = len(body)
        if e.content_range is not None:
            self.headers["content-range"] = e.content_range
        self.write(body)

    def send_json(self, obj):
        """
        Send a JSON response.
        """
        if self._started:
            raise AssertionError("Response already sent")

        self.status_code = OK
        body = json.dumps(obj).encode("utf-8") + b"\n"
        self.headers["content-length"] = len(body)
        self.headers["content-type"] = "application/json"
        self.write(body)

    def close_connection(self):
        """
        Mark the connection for closing when the request completes.
        """
        self.headers["connection"] = "close"
        self._con.close_connection = True

    def write(self, data):
        """
        Write data to the response body.

        NOTE: This method is replaced with the underlying file write method on
        the first call. Do not try to cache this method!
        """
        self._started = True

        b = io.BytesIO()
        self._write_header(b)

        # For small payload, it is faster to copy the data and write in one
        # syscall.
        # TODO: 4096 is a guess, measure what is the best value.
        if len(data) < 4096:
            b.write(data)
            self._con.wfile.write(b.getvalue())
        else:
            self._con.wfile.write(b.getvalue())
            self._con.wfile.write(data)

        # This avoids name lookup on the next calls to write.
        self.write = self._con.wfile.write

    def _write_header(self, b):
        """
        Write HTTP header to buffer b, avoiding one syscall per line in python
        2.7.
        """
        if self.status_code in self._con.responses:
            msg = self._con.responses[self.status_code][0].encode("latin1")
        else:
            msg = b""

        # Write response line.
        b.write(b"%s %d %s\r\n" % (
                self._con.protocol_version.encode("latin1"),
                self.status_code,
                msg))

        # Write default headers.
        b.write(b"server: %s\r\n" %
                self._con.version_string().encode("latin1"))
        b.write(b"date: %s\r\n" %
                self._con.date_time_string().encode("latin1"))

        # Write user headers.
        for name, value in self.headers.items():
            # Encoding entire line to allow using integer value, for example
            # content-length.
            # Note: content-disposition may contain unicode values, so we must
            # encode headers using utf-8.
            b.write(("%s: %s\r\n" % (name, value)).encode("utf-8"))

        # End header.
        b.write(b"\r\n")


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
        Override to enforce lowercase keys. This make it easier to get
        and use values from response headers when processing the
        response.
        """
        dict.__setitem__(self, name.lower(), value)


class Range:
    """
    HTTP Range header.

    See https://tools.ietf.org/html/rfc7233#section-2.1 for details.
    """

    _rx = re.compile(r"bytes=(\d*)-(\d*)$")

    def __init__(self, first, last):
        self.first = first
        self.last = last

    @classmethod
    def parse(cls, header):
        """
        Parse Range header.

        Based on webob.byterange.Range, with several modifications to
        suite our our use case:

        - Raises on invalid or unsupported range so server code does not
          need to do error checking. This also ensure that invalid range will
          not be considered as non-range request which can lead to image
          corruption when the server return the wrong data to the client.

        - Matches better the HTTP spec, using "first" and "last" instead of
          "start" and "stop". The last byte position is kept as is, and not
          incremented by one.

        - Reject invalid range without first or last byte positions, negative
          first, or junk at the end.

        - Does not support case insensitive unit or extra space, since the HTTP
          spec does not specify them.

        - Does not accept None header, only string. We don't want to
          help sloppy programmers.

        Like webob, multiple ranges are not supported, but unlike webob, this
        will reject the entire request, instead of using only the first range.

        Raise:
            http.Error(REQUESTED_RANGE_NOT_SATISFIABLE) if the range is
            invalid, or contain multiple ranges.
        """
        m = cls._rx.match(header)
        if not m:
            raise Error(
                REQUESTED_RANGE_NOT_SATISFIABLE,
                "Cannot satisfy range {!r}, invalid range or multiple ranges"
                .format(header))

        first, last = m.groups()
        if not first:
            if not last:
                # "bytes=-"
                raise Error(
                    REQUESTED_RANGE_NOT_SATISFIABLE,
                    "Cannot satisfy range {!r}, no first or last"
                    .format(header))
            # "bytes=-99"
            return cls(-int(last), None)

        first = int(first)
        if not last:
            # "bytes=0-"
            return cls(first, None)

        last = int(last)
        if first > last:
            raise Error(
                REQUESTED_RANGE_NOT_SATISFIABLE,
                "Cannot satisfy range {!r}, first > last"
                .format(header))

        # "bytes=0-99"
        return cls(first, last)


class ContentRange:
    """
    HTTP ContentRange header.

    See https://tools.ietf.org/html/rfc7233#section-4.2 for details.
    """

    _rx = re.compile(r"bytes (\d+)-(\d+|\*)/(\d+|\*)$")

    def __init__(self, first, last, complete):
        self.first = first
        self.last = last
        self.complete = complete

    @classmethod
    def parse(cls, header):
        """
        Parse ContentRange header.

        This is similar to webob.byterange.ContentRange, with this
        differences:

        - Raises on invalid content range, so server code does not need
          to repeat the error checking. This also ensures that invalid
          header will not be ignored, which will write data to the wrong
          position in an image, corrupting the image.

        - Matches better HTTP spec, using "first", "last", and
          "complete", instead of "start", "stop", and "length", which is
          confusing with content-length.

        - Does not implement the unsatisfied-range format (*/complete)
          which we don't support in a PUT request.

        - Does not accept None, only string. We don't want to help
          sloppy programmers.

        Raise:
            http.Error(BAD_REQUEST) if the range is invalid.
        """
        m = cls._rx.match(header)
        if not m:
            raise Error(
                BAD_REQUEST,
                "Invalid Content-Range {!r}".format(header))

        first, last, complete = m.groups()

        first = int(first)

        if last == "*":
            last = None
        else:
            last = int(last)
            if last < first:
                raise Error(
                    BAD_REQUEST,
                    "Invalid Content-Range {!r}, first > last"
                    .format(header))

        if complete == "*":
            complete = None
        else:
            complete = int(complete)
            if last is not None and last >= complete:
                raise Error(
                    BAD_REQUEST,
                    "Invalid Content-Range {!r}, last >= complete"
                    .format(header))

        return cls(first, last, complete)


class Router:
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
            except Exception as e:
                if isinstance(e, socket.error) and req.connection_lost():
                    log.debug("Client disconnected: %s", e)
                    resp.close_connection()
                elif resp.started:
                    # Already started the response, close the connection.
                    log.exception("Request aborted after starting response")
                    resp.close_connection()
                else:
                    # Don't expose internal errors to client.
                    if not isinstance(e, Error):
                        e = Error(
                            INTERNAL_SERVER_ERROR,
                            "Server failed to perform the request, check logs")
                    # Log the error.
                    if e.code >= INTERNAL_SERVER_ERROR:
                        log.exception("Server error")
                    elif e.code >= BAD_REQUEST:
                        log.error("Client error: %s", e)
                    # Did we read the entire request content?
                    if req.length and req.length > 0:
                        resp.close_connection()
                    resp.send_error(e)

    def dispatch(self, req, resp):
        if req.method not in self.ALLOWED_METHODS:
            raise Error(METHOD_NOT_ALLOWED,
                        "Invalid method {!r}".format(req.method))

        path = req.path
        for route, handler in self._routes:
            match = route.match(path)
            if match:
                try:
                    method = getattr(handler, req.method.lower())
                except AttributeError:
                    raise Error(METHOD_NOT_ALLOWED,
                                "Method {!r} not defined for {!r}"
                                .format(req.method, path))
                return method(req, resp, *match.groups())

        raise Error(NOT_FOUND, "No handler for {!r}".format(path))


# Helpers

def find_addresses(host, port=0):
    # In the past we use "" as a special address to bind to all interfaces.
    # Using "" with socket.getaddrinfo() would result into socket.gaierror. To
    # keep backward compatibility with old configurations, replace "" with "0"
    # which acts in the same way and will be bound to IPv4 interface.
    if host == "":
        host = "0"

    for ai in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM):
        # Filter IPv6 link local addresses. Users can use "localhost" or "::1"
        # to listen to local only address.
        # https://en.wikipedia.org/wiki/Link-local_address
        address = ipaddress.ip_address(ai[4][0])
        if address.is_link_local:
            continue

        yield ai
