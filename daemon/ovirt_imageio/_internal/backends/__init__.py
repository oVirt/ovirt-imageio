# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from collections import namedtuple

from .. import util
from . import file
from . import http
from . import nbd

_modules = {
    "file": file,
    "nbd": nbd,
    "https": http,
}


class Unsupported(Exception):
    """ Requested backend is not supported """


class Context(namedtuple("Context", "backend,buffer")):
    """
    Backend context stored per ticket connection.
    """
    __slots__ = ()

    def close(self):
        try:
            self.backend.close()
        finally:
            self.buffer.close()


def supports(name):
    return name in _modules


def get(req, ticket, config):
    """
    Return a connection backend for this ticket.

    On the first call, open the backend and cache it in the connection context.
    The backend will be closed when the connection is closed.

    Thread safety: requests are accessed by the single connection thread, no
    locking is needed.
    """
    if ticket.uuid not in req.context:
        if not supports(ticket.url.scheme):
            raise Unsupported(
                "Unsupported backend {!r}".format(ticket.url.scheme))

        mode = "r+" if "write" in ticket.ops else "r"
        module = _modules[ticket.url.scheme]

        backend = module.open(
            ticket.url,
            mode,
            sparse=ticket.sparse,
            dirty=ticket.dirty,
            max_connections=config.daemon.max_connections,
            cafile=config.tls.ca_file)

        buf = util.aligned_buffer(config.daemon.buffer_size)

        req.context[ticket.uuid] = Context(backend, buf)

    return req.context[ticket.uuid]
