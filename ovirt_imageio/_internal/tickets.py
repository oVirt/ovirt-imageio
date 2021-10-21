# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
import logging

from . import errors
from . import http
from . import validate

log = logging.getLogger("tickets")


class Handler:
    """
    Handle requests for the /tickets/ resource.
    """

    def __init__(self, config, auth):
        self.config = config
        self.auth = auth

    def get(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        try:
            ticket = self.auth.get(ticket_id)
        except KeyError:
            raise http.Error(
                http.NOT_FOUND, "No such ticket {!r}".format(ticket_id))

        ticket_info = ticket.info()
        log.debug("[%s] GET ticket=%s", req.client_addr, ticket_info)
        resp.send_json(ticket_info)

    def put(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        # TODO: Reject too big ticket json. We know the size of a ticket based
        # on the size of the keys and values.
        try:
            ticket_dict = json.loads(req.read())
        except ValueError as e:
            raise http.Error(
                http.BAD_REQUEST,
                "Ticket is not in a json format: {}".format(e))

        log.info("[%s] ADD ticket=%s", req.client_addr, ticket_dict)
        try:
            self.auth.add(ticket_dict)
        except errors.InvalidTicket as e:
            raise http.Error(
                http.BAD_REQUEST, "Invalid ticket: {}".format(e))

    def patch(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        # TODO: Reject requests with too big payload. We know the size of a
        # ticket patch message based on the keys and values.
        try:
            patch = json.loads(req.read())
        except ValueError as e:
            raise http.Error(
                http.BAD_REQUEST, "Invalid patch: {}".format(e))

        timeout = validate.integer(patch, "timeout", minval=0)

        try:
            ticket = self.auth.get(ticket_id)
        except KeyError:
            raise http.Error(
                http.NOT_FOUND, "No such ticket: {}".format(ticket_id))

        log.info("[%s] EXTEND timeout=%s ticket=%s",
                 req.client_addr, timeout, ticket_id)
        ticket.extend(timeout)

    def delete(self, req, resp, ticket_id):
        """
        Delete a ticket if exists.

        Note that DELETE is idempotent;  the client can issue multiple DELETE
        requests in case of network failures. See
        https://tools.ietf.org/html/rfc7231#section-4.2.2.
        """
        log.info("[%s] REMOVE ticket=%s", req.client_addr, ticket_id)

        if ticket_id:
            try:
                self.auth.remove(ticket_id)
            except errors.TicketCancelTimeout as e:
                # The ticket is still used by some connection so we cannot
                # remove it. The caller can retry the call again when the
                # number connections reach zero.
                raise http.Error(http.CONFLICT, str(e))
        else:
            self.auth.clear()

        resp.status_code = http.NO_CONTENT
