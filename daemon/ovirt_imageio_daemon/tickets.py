# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging

from webob.exc import (
    HTTPBadRequest,
    HTTPNotFound,
)

from ovirt_imageio_common import errors
from ovirt_imageio_common import web

from . import auth

log = logging.getLogger("tickets")


class Handler(object):
    """
    Handle requests for the /tickets/ resource.
    """

    def __init__(self, config, request, clock=None):
        self.config = config
        self.request = request
        self.clock = clock

    def get(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            ticket = auth.get(ticket_id)
        except KeyError:
            raise HTTPNotFound("No such ticket %r" % ticket_id)
        ticket_info = ticket.info()
        log.debug("[%s] GET ticket=%s",
                  web.client_address(self.request), ticket_info)
        return web.response(payload=ticket_info)

    def put(self, ticket_id):
        # TODO
        # - reject invalid or expired ticket
        # - start expire timer
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")

        try:
            ticket_dict = self.request.json
        except ValueError as e:
            raise HTTPBadRequest("Ticket is not in a json format: %s" % e)

        log.info("[%s] ADD ticket=%s",
                 web.client_address(self.request), ticket_dict)
        try:
            auth.add(ticket_dict)
        except errors.InvalidTicket as e:
            raise HTTPBadRequest("Invalid ticket: %s" % e)

        return web.response()

    def patch(self, ticket_id):
        # TODO: restart expire timer
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            patch = self.request.json
        except ValueError as e:
            raise HTTPBadRequest("Invalid patch: %s" % e)
        try:
            timeout = patch["timeout"]
        except KeyError:
            raise HTTPBadRequest("Missing timeout key")
        try:
            timeout = int(timeout)
        except ValueError as e:
            raise HTTPBadRequest("Invalid timeout value: %s" % e)
        try:
            ticket = auth.get(ticket_id)
        except KeyError:
            raise HTTPNotFound("No such ticket: %s" % ticket_id)

        log.info("[%s] EXTEND timeout=%s ticket=%s",
                 web.client_address(self.request), timeout, ticket_id)
        ticket.extend(timeout)
        return web.response()

    def delete(self, ticket_id):
        """
        Delete a ticket if exists.

        Note that DELETE is idempotent;  the client can issue multiple DELETE
        requests in case of network failures. See
        https://tools.ietf.org/html/rfc7231#section-4.2.2.
        """
        # TODO: cancel requests using deleted tickets
        log.info("[%s] REMOVE ticket=%s",
                 web.client_address(self.request), ticket_id)
        if ticket_id:
            try:
                auth.remove(ticket_id)
            except KeyError:
                log.debug("Ticket %s does not exists", ticket_id)
        else:
            auth.clear()
        return web.response(status=204)
