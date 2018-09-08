# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging

import webob
from webob.exc import HTTPBadRequest

from ovirt_imageio_common import directio
from ovirt_imageio_common import errors
from ovirt_imageio_common import validate
from ovirt_imageio_common import web

from . import tickets

log = logging.getLogger("images")


class Handler(object):
    """
    Handle requests for the /images/ resource.
    """

    def __init__(self, config, request, clock=None):
        self.config = config
        self.request = request
        self.clock = clock

    def put(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        size = self.request.content_length
        if size is None:
            raise HTTPBadRequest("Content-Length header is required")
        if size < 0:
            raise HTTPBadRequest("Invalid Content-Length header: %r" % size)
        content_range = web.content_range(self.request)
        offset = content_range.start or 0

        # For backward compatibility, we flush by default.
        flush = validate.enum(self.request.params, "flush", ("y", "n"),
                              default="y")
        flush = (flush == "y")

        ticket = tickets.authorize(ticket_id, "write", offset, size)
        # TODO: cancel copy if ticket expired or revoked
        log.info(
            "[%s] WRITE size=%d offset=%d flush=%s ticket=%s",
            web.client_address(self.request), size, offset, flush, ticket_id)
        op = directio.Receive(
            ticket.url.path,
            self.request.body_file_raw,
            size,
            offset=offset,
            flush=flush,
            buffersize=self.config.daemon.buffer_size,
            clock=self.clock)
        try:
            ticket.run(op)
        except errors.PartialContent as e:
            raise HTTPBadRequest(str(e))
        return web.response()

    def get(self, ticket_id):
        # TODO: cancel copy if ticket expired or revoked
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        # TODO: support partial range (e.g. bytes=0-*)

        offset = 0
        size = None
        if self.request.range:
            offset = self.request.range.start
            if self.request.range.end is not None:
                size = self.request.range.end - offset

        ticket = tickets.authorize(ticket_id, "read", offset, size)
        if size is None:
            size = ticket.size - offset
        log.info(
            "[%s] READ size=%d offset=%d ticket=%s",
            web.client_address(self.request), size, offset, ticket_id)
        op = directio.Send(
            ticket.url.path,
            None,
            size,
            offset=offset,
            buffersize=self.config.daemon.buffer_size,
            clock=self.clock)
        content_disposition = "attachment"
        if ticket.filename:
            filename = ticket.filename.encode("utf-8")
            content_disposition += "; filename=%s" % filename
        resp = webob.Response(
            status=206 if self.request.range else 200,
            app_iter=ticket.bind(op),
            content_type="application/octet-stream",
            content_length=str(size),
            content_disposition=content_disposition,
        )
        if self.request.range:
            content_range = self.request.range.content_range(ticket.size)
            resp.headers["content-range"] = str(content_range)

        return resp

    def patch(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            msg = self.request.json
        except ValueError as e:
            raise HTTPBadRequest("Invalid JSON message: %s" % e)

        op = validate.enum(msg, "op", ("zero", "flush"))
        if op == "zero":
            return self._zero(ticket_id, msg)
        elif op == "flush":
            return self._flush(ticket_id, msg)
        else:
            raise RuntimeError("Unreachable")

    def _zero(self, ticket_id, msg):
        size = validate.integer(msg, "size", minval=0)
        offset = validate.integer(msg, "offset", minval=0, default=0)
        flush = validate.boolean(msg, "flush", default=False)

        ticket = tickets.authorize(ticket_id, "write", offset, size)

        log.info(
            "[%s] ZERO size=%d offset=%d flush=%s ticket=%s",
            web.client_address(self.request), size, offset, flush, ticket_id)
        op = directio.Zero(
            ticket.url.path,
            size,
            offset=offset,
            flush=flush,
            buffersize=self.config.daemon.buffer_size,
            clock=self.clock,
            sparse=ticket.sparse)
        try:
            ticket.run(op)
        except errors.PartialContent as e:
            raise HTTPBadRequest(str(e))
        return web.response()

    def _flush(self, ticket_id, msg):
        ticket = tickets.authorize(ticket_id, "write", 0, 0)
        log.info("[%s] FLUSH ticket=%s",
                 web.client_address(self.request), ticket_id)
        op = directio.Flush(ticket.url.path, clock=self.clock)
        ticket.run(op)
        return web.response()

    def options(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")

        log.info("[%s] OPTIONS ticket=%s",
                 web.client_address(self.request), ticket_id)
        if ticket_id == "*":
            # Reporting the meta-capabilities for all images.
            allow = ["OPTIONS", "GET", "PUT", "PATCH"]
            features = ["zero", "flush"]
        else:
            # Reporting real image capabilities per ticket.
            # This check will fail if the ticket has expired.
            ticket = tickets.authorize(ticket_id, "read", 0, 0)

            # Accessing ticket options considered as client activity.
            ticket.touch()

            allow = ["OPTIONS"]
            features = []
            if ticket.may("read"):
                allow.append("GET")
            if ticket.may("write"):
                allow.extend(("PUT", "PATCH"))
                features = ["zero", "flush"]

        return web.response(
            payload={
                "features": features,
                "unix_socket": self.config.images.socket,
            },
            allow=",".join(allow))
