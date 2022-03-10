# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
http - HTTP backend.
"""

import http.client as http_client
import json
import logging
import os
import socket
import ssl

from .. import errors
from .. import extent
from .. import http

from . common import CLOSED

log = logging.getLogger("backends.http")


def open(url, mode="r+", sparse=True, dirty=False, max_connections=8,
         **options):
    """
    Open a HTTP backend.

    Arguments:
        url (url): parsed HTTPS URL.
        mode (str): ignored, http backend is always read-write.
        sparse (bool): ignored, http backend does not support sparseness.
        dirty (bool): ignored, http backend does not require configuration for
            getting dirty extents.
        max_connections (int): ignored, http backend reports the value
            published by the remote server.
        **options: backend specific options:
            cafile (str): path to CA certificates to trust for certificate
                verification. If not set, trust system's default CA
                certificates instead.
            secure (bool): If False, disable server certificate verification.
            connect_timeout: Time to wait for connection to server.
            read_timeout: Time to wait when reading from server.
    """
    assert url.scheme == "https"
    return Backend(url, **options)


class Backend:

    def __init__(self, url, cafile=None, secure=True, connect_timeout=10,
                 read_timeout=60, connect=True):
        log.debug("Open netloc=%r path=%r cafile=%r secure=%r",
                  url.netloc, url.path, cafile, secure)
        self.url = url
        self._cafile = cafile
        self._secure = secure
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._position = 0
        self._size = None
        self._extents = {}

        # Initlized during connection.
        self._context = None
        self._con = CLOSED
        self._can_extents = False
        self._can_zero = False
        self._can_flush = False
        self._max_readers = 1
        self._max_writers = 1

        if connect:
            self._connect()

    def clone(self):
        """
        Return new backend connected to same server.
        """
        con = self._clone_connection()
        try:
            # Create a disconnected backend.
            backend = self.__class__(
                self.url,
                cafile=self._cafile,
                secure=self._secure,
                connect_timeout=self._connect_timeout,
                read_timeout=self._read_timeout,
                connect=False)

            # Use cloned connection.
            backend._con = con

            # Copy state from current backend since we are not going to check
            # server capabilities.
            backend._context = self._context
            backend._can_extents = self._can_extents
            backend._can_zero = self._can_zero
            backend._can_flush = self._can_flush
            backend._max_readers = self._max_readers
            backend._max_writers = self._max_writers

            # Copy size and extents to save expensive EXTENTS calls.
            backend._size = self._size
            for ctx in list(self._extents):
                backend._extents[ctx] = self._extents[ctx].copy()

            return backend
        except Exception:
            con.close()
            raise

    def _connect(self):
        self._context = self._create_ssl_context()
        self._con = self._create_tcp_connection()
        try:
            options = self._options()
            log.debug("Server options: %s", options)
            self._can_extents = options.get("extents", False)
            self._can_zero = options.get("zero", False)
            self._can_flush = options.get("flush", False)

            # In oVirt 4.3 qemu-nbd was configured to allow only single
            # connection, so practicaly we can have only single reader.
            self._max_readers = options.get("max_readers", 1)

            # For safety, assume that old server that does not publish
            # max_writers does not support multiple writers.
            self._max_writers = options.get("max_writers", 1)

            self._optimize_connection(options.get("unix_socket"))
        except Exception:
            self._con.close()
            self._con = CLOSED
            raise

    @property
    def name(self):
        return "http"

    @property
    def block_size(self):
        return 1

    @property
    def max_readers(self):
        return self._max_readers

    @property
    def max_writers(self):
        return self._max_writers

    # Preferred interface.

    def read_from(self, reader, length, buf):
        """
        Send PUT request and stream length bytes from reader to the HTTP
        server.

        Arguments:
            reader (object): must implement readinto(buf)
            length (int): number of bytes to read from reader
            buf (buffer): buffer to used for reading and writing.
        """
        self._put_header(length)

        with memoryview(buf) as view:
            max_step = len(view)
            todo = length
            while todo:
                step = min(todo, max_step)
                n = reader.readinto(view[:step])
                if n == 0:
                    raise RuntimeError(
                        "Expected {} bytes, got {} bytes"
                        .format(length, length - todo))
                try:
                    self._con.send(view[:n])
                except (BrokenPipeError, ConnectionResetError):
                    # Server closed the connection, but it may have sent a
                    # helpful error message.
                    break
                todo -= n

        res = self._con.getresponse()

        if res.status != http_client.OK:
            self._reraise(res.status, res.read())

        res.read()
        self._position += length
        return length

    def write_to(self, writer, length, buf):
        """
        Send GET request and stream length bytes to writer.

        Arguments:
            writer (object): must implement write(buf)
            length (int): number of bytes to read from reader
            buf (buffer): buffer to used for reading and writing.
        """
        res = self._get(length)

        with memoryview(buf) as view:
            max_step = len(view)
            todo = length
            while todo:
                step = min(todo, max_step)
                n = res.readinto(view[:step])
                if n == 0:
                    raise RuntimeError(
                        "Expected {} bytes, got {} bytes"
                        .format(length, length - todo))
                writer.write(view[:n])
                todo -= n

        self._position += length
        return length

    # Generic interface.

    def readinto(self, buf):
        """
        Send GET request, reading bytes at current position into buf.
        """
        length = min(len(buf), self.size() - self._position)
        if length <= 0:
            # Zero length Range (first > last) is invalid.
            # https://tools.ietf.org/html/rfc7233#section-2.1
            return 0

        res = self._get(length)

        with memoryview(buf)[:length] as view:
            self._read_all(res, view)

        self._position += length
        return length

    def write(self, buf):
        """
        Send PUT request, writing buf contents at current position.
        """
        length = len(buf)
        self._put_header(length)

        try:
            self._con.send(buf)
        except (BrokenPipeError, ConnectionResetError):
            # Server closed the connection, but it may have sent a helpful
            # error message.
            pass

        res = self._con.getresponse()

        if res.status != http_client.OK:
            self._reraise(res.status, res.read())

        res.read()
        self._position += length
        return length

    def zero(self, length):
        """
        Send PATCH/zero request, writing zeroes at current position.
        """
        if not self._can_zero:
            return self._emulate_zero(length)

        msg = {
            "op": "zero",
            "offset": self._position,
            "size": length,
            "flush": not self._can_flush
        }
        self._patch(msg)

        self._position += length
        return length

    def flush(self):
        """
        Send a PATCH/flush request, flushing changes to storage.
        """
        if self._can_flush:
            self._patch({"op": "flush"})

    def extents(self, context="zero"):
        """
        Get image extents, return iterator over received extents.
        """
        if context not in ("zero", "dirty"):
            raise RuntimeError("Invalid context: {}".format(context))

        if not self._can_extents:
            if context == "zero":
                yield extent.ZeroExtent(0, self.size(), False, False)
                return
            else:
                raise errors.UnsupportedOperation(
                    "Server does not support dirty extents")

        if context not in self._extents:
            self._extents[context] = list(self._get_extents(context))

        for ext in self._extents[context]:
            yield ext

    def tell(self):
        return self._position

    def seek(self, n, how=os.SEEK_SET):
        if how == os.SEEK_SET:
            self._position = n
        elif how == os.SEEK_CUR:
            self._position += n
        elif how == os.SEEK_END:
            self._position = self.size() + n
        return self._position

    def size(self):
        # We have 2 bad options:
        # - Get last extent, may be slow, and may not be neded otherwise.
        # - Emulate HEAD request, logging tracebacks in the remote server.
        # Getting extents is more polite, so lets use it if we can.
        if self._size is None:
            if self._can_extents:
                last = list(self.extents())[-1]
                self._size = last.start + last.length
            else:
                self._size = self._emulate_head()

        return self._size

    def close(self):
        if self._con is not CLOSED:
            log.debug("Close netloc=%r path=%r",
                      self.url.netloc, self.url.path)
            self._con.close()
            self._con = CLOSED

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        try:
            self.close()
        except Exception:
            # Do not hide the original error.
            if t is None:
                raise
            log.exception("Error closing backend")

    # Debugging interface

    @property
    def server_address(self):
        return self._con.server_address

    # Private

    def _create_ssl_context(self):
        context = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH, cafile=self._cafile)

        if not self._secure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        return context

    def _create_tcp_connection(self):
        log.debug("Connecting to tcp socket %r", self.url.netloc)
        con = HTTPSConnection(
            self.url.netloc,
            timeout=self._connect_timeout,
            context=self._context)
        try:
            con.connect()
            con.sock.settimeout(self._read_timeout)
        except Exception:
            con.close()
            raise

        return con

    def _create_unix_connection(self, unix_socket):
        log.debug("Connecting to unix socket %r", unix_socket)
        con = UnixHTTPConnection(
            unix_socket, timeout=self._connect_timeout)
        try:
            con.connect()
            con.sock.settimeout(self._read_timeout)
        except Exception:
            con.close()
            raise

        return con

    def _optimize_connection(self, unix_socket):
        """
        Try to switch to Unix socket for improved performane. If we fail to
        switch continue to use HTTPS.
        """
        if not (self._con.is_local() and unix_socket):
            return

        try:
            con = self._create_unix_connection(unix_socket)
        except Exception as e:
            log.warning("Cannot use unix socket: %s", e)
        else:
            self._con.close()
            self._con = con

    def _clone_connection(self):
        if isinstance(self._con, HTTPSConnection):
            return self._create_tcp_connection()
        else:
            return self._create_unix_connection(self.server_address)

    def _get(self, length):
        headers = {}
        headers["range"] = "bytes={}-{}".format(
            self._position, self._position + length - 1)

        self._con.request("GET", self.url.path, headers=headers)
        res = self._con.getresponse()

        if res.status != http_client.PARTIAL_CONTENT:
            self._reraise(res.status, res.read())

        content_length = int(res.getheader("content-length"))
        if content_length != length:
            raise RuntimeError(
                "Unexpected content_length={} expected={}"
                .format(content_length, length))

        return res

    def _put_header(self, length):
        path = self.url.path
        if self._can_flush:
            path += "?flush=n"

        self._con.putrequest("PUT", path)

        self._con.putheader("content-length", length)
        self._con.putheader("content-type", "application/octet-stream")
        self._con.putheader("content-range", "bytes {}-{}/*".format(
                self._position, self._position + length - 1))

        self._con.endheaders()

    def _patch(self, msg):
        body = json.dumps(msg).encode("utf-8")
        headers = {"content-type": "application/json"}

        self._con.request("PATCH", self.url.path, body=body, headers=headers)
        res = self._con.getresponse()

        if res.status != http_client.OK:
            self._reraise(res.status, res.read())

        res.read()

    def _options(self):
        self._con.request("OPTIONS", self.url.path)
        res = self._con.getresponse()
        body = res.read()

        options = {}

        if res.status == http_client.METHOD_NOT_ALLOWED:
            # Older daemon did not implement OPTIONS
            return options
        elif res.status == http_client.NO_CONTENT:
            # Older proxy did implement OPTIONS but does not return any
            # content.
            return options
        elif res.status != http_client.OK:
            raise self._reraise(res.status, body)

        # New daemon or proxy provides options dict.
        try:
            options = json.loads(body.decode("utf-8"))
        except ValueError:
            # Bad response, we must assume we don't support any features or
            # unix socket.
            return options

        # Flaten features into options dict to make it easier to consume.  If
        # we get invalid response without feature list, assume the server does
        # not support any feature.
        for feature in options.pop("features", []):
            options[feature] = True

        return options

    def _get_extents(self, context):
        self._con.request("GET", self.url.path + "/extents?context=" + context)
        res = self._con.getresponse()
        data = res.read()

        if res.status == http_client.NOT_FOUND:
            raise errors.UnsupportedOperation(
                "Server does not support {} extents: {}"
                .format(context, data[:512]))

        if res.status != http_client.OK:
            self._reraise(res.status, data)

        extents = json.loads(data.decode("utf-8"))

        cls = extent.ZeroExtent if context == "zero" else extent.DirtyExtent
        for ext in extents:
            yield cls.from_dict(ext)

    def _emulate_head(self):
        """
        Emulate HEAD request by sending GET and closing the connction without
        reading anything. This is not very polite, but we don't have another
        choice if the server does not support extents.

        NOTE: Logs noisy tracebacks in the daemon logs.
        """
        self._con.request("GET", self.url.path)
        res = self._con.getresponse()

        if res.status != http_client.OK:
            self._reraise(res.status, res.read())

        size = int(res.getheader("content-length"))

        # The connection will automaticlaly reconnect on the next request.
        self._con.close()

        return size

    def _emulate_zero(self, length):
        """
        Emulate PATCH/zero with PUT for old server without zero support.
        """
        self._put_header(length)

        buf = bytearray(128 * 1024)
        todo = length
        while todo > len(buf):
            self._con.send(buf)
            todo -= len(buf)
        self._con.send(memoryview(buf)[:todo])

        res = self._con.getresponse()

        if res.status != http_client.OK:
            self._reraise(res.status, res.read())

        res.read()
        self._position += length
        return length

    def _read_all(self, res, buf):
        with memoryview(buf) as view:
            length = len(view)
            pos = 0
            while pos < length:
                n = res.readinto(view[pos:])
                if n == 0:
                    raise RuntimeError(
                        "Expected {} byes, got {} bytes".format(length, pos))
                pos += n

    def _reraise(self, status, body):
        """
        Reconstruct http.Error from daemon response and raise it.

        Assume that body is text using utf-8 encoding. Invaid characters will
        be replaed with the unicode replacement character.

        Trim large body since it cannot be a valid error message.
        """
        # Errors are always terminated by newline. Remove the newline before
        # raising to avoid double newlines.
        msg = body[:512].decode("utf-8", errors="replace").rstrip()
        raise http.Error(status, msg)


class HTTPSConnection(http_client.HTTPSConnection):
    """
    Enhanced HTTPS connection.
    """

    def is_local(self):
        """
        Return True if connected to the local host.
        """
        # Hack for daemon versions 1.4.0 and 1.4.1 that supported unix
        # socket but not keep alive connections. With these versions the
        # socket is closed after calling getresponse().
        if self.sock is None:
            self.connect()

        return self.sock.getsockname()[0] == self.sock.getpeername()[0]

    @property
    def server_address(self):
        # Returned value depends on the address family: for IPv4 is a tuple of
        # two value while for IPv6 is a tuple of four values. To make server
        # address uniform, ensure it's always a tuple of two values -
        # hostname/IP address and port number.
        return self.sock.getpeername()[:2]


class UnixHTTPConnection(http_client.HTTPConnection):
    """
    HTTP connection over unix domain socket.
    """

    def __init__(self, path, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.path = path
        super().__init__("localhost", timeout=timeout)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
            self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)

    @property
    def server_address(self):
        return self.path
