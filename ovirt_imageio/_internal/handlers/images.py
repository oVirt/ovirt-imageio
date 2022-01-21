# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
import logging

from .. import backends
from .. import cors
from .. import errors
from .. import http
from .. import ops
from .. import validate

log = logging.getLogger("images")

BASE_FEATURES = ("checksum", "extents")
ALL_FEATURES = BASE_FEATURES + ("flush", "zero")


class Handler:
    """
    Handle requests for the /images/ resource.
    """

    def __init__(self, config, auth):
        self.config = config
        self.auth = auth

    @cors.allow()
    def put(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        # Used by engine UI to close connection after upload.
        close = req.query.get("close") == "y"
        if close:
            resp.close_connection()

        size = req.content_length
        if size is None:
            raise http.Error(
                http.BAD_REQUEST, "Content-Length header is required")

        offset = req.content_range.first if req.content_range else 0

        # For backward compatibility, we flush by default.
        flush = validate.enum(req.query, "flush", ("y", "n"), default="y")
        flush = (flush == "y")

        try:
            ticket = self.auth.authorize(ticket_id, "write")
            ctx = backends.get(req, ticket, self.config)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e))

        validate.allowed_range(offset, size, ticket)

        log.debug(
            "[%s] WRITE size=%d offset=%d flush=%s close=%s transfer=%s",
            req.client_addr, size, offset, flush, close, ticket.transfer_id)

        op = ops.Write(
            ctx.backend,
            req,
            ctx.buffer,
            size,
            offset=offset,
            flush=flush,
            clock=req.clock)
        try:
            ticket.run(op)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e)) from None
        except errors.PartialContent as e:
            raise http.Error(http.BAD_REQUEST, str(e))

    @cors.allow()
    def get(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        # Used by engine UI to close connection after download.
        close = req.query.get("close") == "y"
        if close:
            resp.close_connection()

        offset = 0
        size = None
        if req.range:
            offset = req.range.first
            if offset < 0:
                # TODO: support suffix-byte-range-spec "bytes=-last".
                # See https://tools.ietf.org/html/rfc7233#section-2.1.
                raise http.Error(
                    http.REQUESTED_RANGE_NOT_SATISFIABLE,
                    "suffix-byte-range-spec not supported yet")

            if req.range.last is not None:
                # Add 1 to include the last byte in the payload.
                size = req.range.last - offset + 1
                # TODO: validate size with actual image size.

        try:
            ticket = self.auth.authorize(ticket_id, "read")
            ctx = backends.get(req, ticket, self.config)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e))

        if size is not None:
            validate.allowed_range(offset, size, ticket)
            validate.available_range(offset, size, ticket, ctx.backend)
        else:
            size = min(ticket.size, ctx.backend.size()) - offset

        log.debug(
            "[%s] READ size=%d offset=%d close=%s transfer=%s",
            req.client_addr, size, offset, close, ticket.transfer_id)

        content_disposition = "attachment"
        if ticket.filename:
            content_disposition += "; filename=%s" % ticket.filename

        resp.headers["content-length"] = size
        resp.headers["content-type"] = "application/octet-stream"
        resp.headers["content-disposition"] = content_disposition

        if req.range:
            resp.status_code = http.PARTIAL_CONTENT
            resp.headers["content-range"] = "bytes %d-%d/%d" % (
                offset, offset + size - 1, ticket.size)

        op = ops.Read(
            ctx.backend,
            resp,
            ctx.buffer,
            size,
            offset=offset,
            clock=req.clock)
        try:
            ticket.run(op)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e)) from None
        except errors.PartialContent as e:
            raise http.Error(http.BAD_REQUEST, str(e))

    def patch(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        # TODO: Reject requests with too big payloads. We know the maximum size
        # of a payload based on the keys and the size of offset and size.
        try:
            msg = json.loads(req.read())
        except ValueError as e:
            raise http.Error(
                http.BAD_REQUEST, "Invalid JSON message {}" .format(e))

        op = validate.enum(msg, "op", ("zero", "flush"))
        if op == "zero":
            return self._zero(req, resp, ticket_id, msg)
        elif op == "flush":
            return self._flush(req, resp, ticket_id, msg)
        else:
            raise RuntimeError("Unreachable")

    def _zero(self, req, resp, ticket_id, msg):
        size = validate.integer(msg, "size", minval=0)
        offset = validate.integer(msg, "offset", minval=0, default=0)
        flush = validate.boolean(msg, "flush", default=False)

        try:
            ticket = self.auth.authorize(ticket_id, "write")
            ctx = backends.get(req, ticket, self.config)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e))

        validate.allowed_range(offset, size, ticket)

        log.debug(
            "[%s] ZERO size=%d offset=%d flush=%s transfer=%s",
            req.client_addr, size, offset, flush, ticket.transfer_id)

        op = ops.Zero(
            ctx.backend,
            size,
            offset=offset,
            flush=flush,
            clock=req.clock)

        try:
            ticket.run(op)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e)) from None
        except errors.PartialContent as e:
            raise http.Error(http.BAD_REQUEST, str(e))

    def _flush(self, req, resp, ticket_id, msg):
        try:
            ticket = self.auth.authorize(ticket_id, "write")
            ctx = backends.get(req, ticket, self.config)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e))

        log.info("[%s] FLUSH transfer=%s",
                 req.client_addr, ticket.transfer_id)

        op = ops.Flush(ctx.backend, clock=req.clock)

        try:
            ticket.run(op)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e)) from None

    @cors.allow(allow_methods="OPTIONS,GET,PUT")
    def options(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        options = {}

        if self.config.local.enable:
            options["unix_socket"] = self.config.local.socket

        if ticket_id == "*":
            log.info("[%s] OPTIONS transfer=*", req.client_addr)
            # Reporting the meta-capabilities for all images.
            allow = ["OPTIONS", "GET", "PUT", "PATCH"]
            options["features"] = ALL_FEATURES
        else:
            # Reporting real image capabilities per ticket.
            try:
                ticket = self.auth.authorize(ticket_id, "read")
                ctx = backends.get(req, ticket, self.config)
            except errors.AuthorizationError as e:
                resp.close_connection()
                raise http.Error(http.FORBIDDEN, str(e))

            log.info("[%s] OPTIONS transfer=%s",
                     req.client_addr, ticket.transfer_id)

            # Accessing ticket options considered as client activity.
            ticket.touch()

            allow = ["OPTIONS"]

            if ticket.may("read"):
                allow.append("GET")
                options["features"] = BASE_FEATURES

            if ticket.may("write"):
                allow.extend(("PUT", "PATCH"))
                options["features"] = ALL_FEATURES

            # Backend specific options.
            options["max_readers"] = ctx.backend.max_readers
            options["max_writers"] = ctx.backend.max_writers

        resp.headers["allow"] = ",".join(allow)
        resp.send_json(options)
