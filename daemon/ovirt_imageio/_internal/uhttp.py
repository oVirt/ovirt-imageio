# ovirt-imageio
# Copyright (C) 2015-2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import http.client as http_client
import logging
import os
import socket

from . import http
from . import util

PUT = "PUT"
DELETE = "DELETE"
PATCH = "PATCH"
GET = "GET"

log = logging.getLogger("uhttp")


class UnsupportedError(Exception):
    pass


class _UnixMixin:

    def set_tunnel(self, host, port=None, headers=None):
        raise UnsupportedError("Tunneling is not supported")


class UnixHTTPConnection(_UnixMixin, http_client.HTTPConnection):
    """
    HTTP connection over unix domain socket.
    """

    def __init__(self, path, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.path = path
        super().__init__("localhost", timeout=timeout)

    def connect(self):
        self.sock = _create_unix_socket(self.timeout)
        self.sock.connect(self.path)


class Server(http.Server):
    """
    HTTP server over unix domain socket.
    """

    address_family = socket.AF_UNIX
    server_name = "localhost"
    server_port = None

    def create_socket(self, prefer_ipv4=False):
        self.socket = socket.socket(socket.AF_UNIX, self.socket_type)

        if self.server_address == "":
            # User wants to bind to a random abstract socket.
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)

    def server_bind(self):
        """
        Override to remove existing socket for pathname sockets, and support
        random abstract sockets.  See unix(7) for details.
        """
        if self.server_address and self.server_address[0] != "\0":
            # A pathname socket must be removed before binding.
            self._remove_socket()

        self.socket.bind(self.server_address)
        # Socket addresses in Linux abstract namespace are returned as bytes,
        # so we have to eventually convert address to string.
        # See https://docs.python.org/3.9/library/socket.html#socket-families
        self.server_address = util.ensure_text(self.socket.getsockname())

    def get_request(self):
        """
        Override to return non-empty client address, expected by
        WSGIRequestHandler.
        """
        sock, _ = self.socket.accept()
        return sock, self.server_address

    def shutdown(self):
        if self.server_address[0] != "\0":
            self._remove_socket()
        super().shutdown()

    def _remove_socket(self):
        log.debug("Removing socket %r", self.server_address)
        try:
            os.unlink(self.server_address)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise


class Connection(http.Connection):
    """
    HTTP connection over unix domain socket.
    """

    # Not needed for unix socket.
    disable_nagle_algorithm = False

    def address_string(self):
        """
        Override to return meaningfull string instead of the first character of
        the unix socket path.
        """
        return "local"


def _create_unix_socket(timeout):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
        sock.settimeout(timeout)
    return sock
