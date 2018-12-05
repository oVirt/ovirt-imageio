# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from . import file


def open(ticket):
    """
    Open a backend for this ticket.
    """
    # TODO: use ticket.url.scheme to select the backend.
    mode = "r+" if "write" in ticket.ops else "r"
    return file.open(ticket.url.path, mode, sparse=ticket.sparse)
