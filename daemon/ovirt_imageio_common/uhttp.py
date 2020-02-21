# ovirt-imageio
# Copyright (C) 2015-2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import os
import socket

import six
from six.moves import http_client

from . import http
from . import util

PUT = "PUT"
DELETE = "DELETE"
PATCH = "PATCH"
GET = "GET"


class UnsupportedError(Exception):
    pass


class _UnixMixin(object):

    def set_tunnel(self, host, port=None, headers=None):
        raise UnsupportedError("Tunneling is not supported")


class UnixHTTPConnection(_UnixMixin, http_client.HTTPConnection):
    """
    HTTP connection over unix domain socket.
    """

    def __init__(self, path, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.path = path
        extra = {}
        if six.PY2:
            extra['strict'] = True
        http_client.HTTPConnection.__init__(self, "localhost", timeout=timeout,
                                            **extra)

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

    def server_bind(self):
        """
        Override to remove existing socket for pathname sockets, and support
        random abstract sockets.  See unix(7) for details.
        """
        if self.server_address == "":
            # User wants to bind to a random abstract socket.
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        elif self.server_address[0] != "\0":
            # A pathname socket must be removed before binding.
            try:
                os.unlink(self.server_address)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

        self.socket.bind(self.server_address)
        self.server_address = util.ensure_text(self.socket.getsockname())

    def get_request(self):
        """
        Override to return non-empty client address, expected by
        WSGIRequestHandler.
        """
        sock, _ = self.socket.accept()
        return sock, self.server_address


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
