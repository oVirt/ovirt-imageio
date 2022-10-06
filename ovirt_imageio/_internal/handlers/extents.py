# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import logging

from .. import backends
from .. import errors
from .. import http
from .. import validate

log = logging.getLogger("extents")


class Handler:
    """
    Handle requests for the /images/ticket-id/extents resource.
    """

    def __init__(self, config, auth):
        self.config = config
        self.auth = auth

    def get(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        try:
            ticket = self.auth.authorize(ticket_id, "read")
            ctx = backends.get(req, ticket, self.config)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e))

        context = validate.enum(
            req.query, "context", ("zero", "dirty"), default="zero")

        if context == "dirty" and not ticket.dirty:
            raise http.Error(
                http.NOT_FOUND, "Ticket does not support dirty extents")

        log.info("[%s] EXTENTS transfer=%s context=%s",
                 req.client_addr, ticket.transfer_id, context)

        with req.clock.run("extents"):
            try:
                extents = [ext.to_dict()
                           for ext in ctx.backend.extents(context=context)]
            except errors.UnsupportedOperation as e:
                raise http.Error(http.NOT_FOUND, str(e))

        resp.send_json(extents)
