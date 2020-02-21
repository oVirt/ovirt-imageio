# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from . import file
from . import nbd

_modules = {"file": file, "nbd": nbd}


class Unsupported(Exception):
    """ Requested backend is not supported """


def supports(name):
    return name in _modules


def get(req, ticket):
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
            dirty=ticket.dirty)

        req.context[ticket.uuid] = backend

    return req.context[ticket.uuid]
