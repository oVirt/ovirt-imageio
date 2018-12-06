# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from . import file


def get(req, ticket, buffer_size=1024**2):
    """
    Return a connection backend for this ticket.

    On the first call, open the backend and cache it in the connection context.
    The backend will be closed when the connection is closed.

    Thread safety: requests are accessed by the single connection thread, no
    locking is needed.
    """
    if ticket.uuid not in req.context:
        # TODO: use ticket.url.scheme to select the backend.
        mode = "r+" if "write" in ticket.ops else "r"

        backend = file.open(
            ticket.url.path,
            mode,
            sparse=ticket.sparse,
            buffer_size=buffer_size)

        req.context[ticket.uuid] = backend

    return req.context[ticket.uuid]
