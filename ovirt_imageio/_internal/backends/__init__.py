# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from collections import namedtuple
from functools import partial

from .. import errors
from .. import util

from . common import CLOSED
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


class Closer:
    """
    Call function when closed.
    """
    def __init__(self, func):
        self.close = func


class Wrapper:
    """
    Used to lend a backend without closing the wrapped backend.
    """

    def __init__(self, backend):
        self._backend = backend

    def close(self):
        self._backend = CLOSED

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()

    def __getattr__(self, name):
        return getattr(self._backend, name)


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
    try:
        return ticket.get_context(req.connection_id)
    except KeyError:
        if not supports(ticket.url.scheme):
            raise Unsupported(
                "Unsupported backend {!r}".format(ticket.url.scheme))

        mode = "r+" if "write" in ticket.ops else "r"
        module = _modules[ticket.url.scheme]

        # If HTTP backend has no explict CA file configuration, use CA file
        # from TLS configuration.
        ca_file = config.backend_http.ca_file or config.tls.ca_file

        backend = module.open(
            ticket.url,
            mode=mode,
            sparse=ticket.sparse,
            dirty=ticket.dirty,
            max_connections=config.daemon.max_connections,
            cafile=ca_file)

        backend_config = getattr(config, "backend_" + backend.name)
        buf = util.aligned_buffer(backend_config.buffer_size)
        ctx = Context(backend, buf)

        # Keep the context in the ticket so we monitor the number of
        # connections using the ticket.
        try:
            ticket.add_context(req.connection_id, ctx)
        except errors.AuthorizationError:
            # The ticket was canceled. close the backend and let the caller
            # handle the failure.
            ctx.close()
            raise

        # Authorized connections get a longer timeout from the ticket.
        req.set_connection_timeout(ticket.inactivity_timeout)

        # Register a closer removing the context when the connection is closed.
        req.context[ticket.uuid] = Closer(
            partial(ticket.remove_context, req.connection_id))

        return ctx
