# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import socket

from contextlib import closing

log = logging.getLogger("test")


def fill(b, size):
    count, rest = divmod(size, len(b))
    return b * count + b[:rest]


BUFFER = fill(b"ABCDEFGHIJ", 1024**2)
BLOCK = fill(b"abcdefghij", 512)
BYTES = fill(b"0123456789", 42)


def head(b):
    return b[:10]


def random_tcp_port():
    """
    Find a random (likely) unused port.

    The port is unused when the call return, but another process may
    grab it.  If you don't control the environmemnt, be prepared for
    bind failures.
    """
    s = socket.socket()
    with closing(s):
        s.bind(("localhost", 0))
        port = s.getsockname()[1]
        log.debug("Found unused TCP port %s", port)
        return port
