# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import errno
import logging
import socket
import time

from contextlib import closing

from . import ipv6

log = logging.getLogger("sockutil")


class UnixAddress(str):
    """
    A unix socket path with additioal methods to make it easier to handle both
    unix socket and TCP socket in the same code.

    Because we inherit from str, you can pass an instance to socket.connect()
    or socket.bind().
    """

    @property
    def transport(self):
        return "unix"

    @property
    def path(self):
        return str(self)


class TCPAddress(tuple):
    """
    A TCP socket 2 tuple (host, port) with additioal methods to make it easier
    to handle both unix socket and TCP socket in the same code.

    Because we inherit from tuple, you can pass an instance to socket.connect()
    or socket.bind().
    """

    def __new__(cls, host, port):
        if not isinstance(host, str):
            raise ValueError("Invalid host {!r}, expecting string value"
                             .format(host))
        if not isinstance(port, int):
            raise ValueError("Invalid port {!r}, expecting integer value"
                             .format(port))
        host = ipv6.unquote_address(host)
        return tuple.__new__(cls, (host, port))

    @property
    def transport(self):
        return "tcp"

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]


def wait_for_socket(addr, timeout, step=0.02):
    """
    Wait until socket is available.

    Arguments:
        addr (sockutil.Address): server address
        timeout (double): time to wait for socket
        step (double): check internal

    Return True if socket is available, False if socket is not available within
    the requested timeout.
    """
    start = time.monotonic()
    deadline = start + timeout

    log.debug("Waiting for socket %s up to %.6f seconds", addr, timeout)

    while True:
        try:
            check_connection(addr)
        except socket.error as e:
            if e.args[0] not in (errno.ECONNREFUSED, errno.ENOENT):
                raise

            # Timed out?
            now = time.monotonic()
            if now >= deadline:
                return False

            # Wait until the next iteration, but not more than the
            # requested deadline.
            wait = min(step, deadline - now)
            time.sleep(wait)
        else:
            log.debug("Waited for %s %.6f seconds",
                      addr, time.monotonic() - start)
            return True


def check_connection(addr):
    if addr.transport == "unix":
        sock = socket.socket(socket.AF_UNIX)
        with closing(sock):
            sock.connect(addr)
    elif addr.transport == "tcp":
        sock = socket.create_connection(addr)
        sock.close()
    else:
        raise RuntimeError("Cannot wait for {}".format(addr))
