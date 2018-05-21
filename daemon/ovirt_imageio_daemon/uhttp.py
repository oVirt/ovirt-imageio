# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
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

from . import wsgi

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


class UnixWSGIServer(wsgi.WSGIServer):
    """
    WSGI HTTP server over unix domain socket.
    """

    address_family = socket.AF_UNIX
    server_name = "localhost"
    server_port = None

    def server_bind(self):
        """
        Override to remove existing socket.
        """
        try:
            os.unlink(self.server_address)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        self.socket.bind(self.server_address)
        self.setup_environ()

    def get_request(self):
        """
        Override to return non-empty client address, expected by
        WSGIRequestHandler.
        """
        sock, _ = self.socket.accept()
        return sock, self.server_address


class UnixWSGIRequestHandler(wsgi.WSGIRequestHandler):
    """
    WSGI HTTP request handler over unix domain socket.
    """

    def address_string(self):
        """
        Override to avoid pointless code in WSGIRequestHandler assuming AF_INET
        socket address (host, port).
        """
        return "localhost"


def _create_unix_socket(timeout):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
        sock.settimeout(timeout)
    return sock
