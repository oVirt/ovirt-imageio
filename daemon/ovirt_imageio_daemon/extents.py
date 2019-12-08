# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging

from ovirt_imageio_common import backends
from ovirt_imageio_common import errors
from ovirt_imageio_common import http

from . import auth

log = logging.getLogger("extents")


class Handler(object):
    """
    Handle requests for the /images/ticket-id/extents resource.
    """

    def __init__(self, config):
        self.config = config

    def get(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        try:
            ticket = auth.authorize(ticket_id, "read")
        except errors.AuthorizationError as e:
            raise http.Error(http.FORBIDDEN, str(e))

        log.info("[%s] EXTENTS ticket=%s", req.client_addr, ticket_id)

        backend = backends.get(
            req, ticket, buffer_size=self.config.daemon.buffer_size)

        extents = [
            {"start": ext.start, "length": ext.length, "zero": ext.zero}
            for ext in backend.extents()
        ]

        resp.send_json(extents)
