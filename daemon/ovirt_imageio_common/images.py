# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import json
import logging

from . import auth
from . import backends
from . import ops
from . import errors
from . import http
from . import validate

log = logging.getLogger("images")


class Handler(object):
    """
    Handle requests for the /images/ resource.
    """

    def __init__(self, config):
        self.config = config

    def put(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        size = req.content_length
        if size is None:
            raise http.Error(
                http.BAD_REQUEST, "Content-Length header is required")

        offset = req.content_range.first if req.content_range else 0

        # For backward compatibility, we flush by default.
        flush = validate.enum(req.query, "flush", ("y", "n"), default="y")
        flush = (flush == "y")

        try:
            ticket = auth.authorize(ticket_id, "write")
        except errors.AuthorizationError as e:
            raise http.Error(http.FORBIDDEN, str(e))

        validate.allowed_range(offset, size, ticket)

        log.info(
            "[%s] WRITE size=%d offset=%d flush=%s ticket=%s",
            req.client_addr, size, offset, flush, ticket_id)

        op = ops.Receive(
            backends.get(req, ticket),
            req,
            size,
            offset=offset,
            flush=flush,
            buffersize=self.config.daemon.buffer_size,
            clock=req.clock)
        try:
            ticket.run(op)
        except errors.PartialContent as e:
            raise http.Error(http.BAD_REQUEST, str(e))

    def get(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

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
            ticket = auth.authorize(ticket_id, "read")
        except errors.AuthorizationError as e:
            raise http.Error(http.FORBIDDEN, str(e))

        backend = backends.get(req, ticket)

        if size is not None:
            validate.allowed_range(offset, size, ticket)
            validate.available_range(offset, size, ticket, backend)
        else:
            size = min(ticket.size, backend.size()) - offset

        log.info(
            "[%s] READ size=%d offset=%d ticket=%s",
            req.client_addr, size, offset, ticket_id)

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

        op = ops.Send(
            backends.get(req, ticket),
            resp,
            size,
            offset=offset,
            buffersize=self.config.daemon.buffer_size,
            clock=req.clock)
        try:
            ticket.run(op)
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
            ticket = auth.authorize(ticket_id, "write")
        except errors.AuthorizationError as e:
            raise http.Error(http.FORBIDDEN, str(e))

        validate.allowed_range(offset, size, ticket)

        log.info(
            "[%s] ZERO size=%d offset=%d flush=%s ticket=%s",
            req.client_addr, size, offset, flush, ticket_id)

        op = ops.Zero(
            backends.get(req, ticket),
            size,
            offset=offset,
            flush=flush,
            buffersize=self.config.daemon.buffer_size,
            clock=req.clock)
        try:
            ticket.run(op)
        except errors.PartialContent as e:
            raise http.Error(http.BAD_REQUEST, str(e))

    def _flush(self, req, resp, ticket_id, msg):
        try:
            ticket = auth.authorize(ticket_id, "write")
        except errors.AuthorizationError as e:
            raise http.Error(http.FORBIDDEN, str(e))

        log.info("[%s] FLUSH ticket=%s", req.client_addr, ticket_id)

        op = ops.Flush(backends.get(req, ticket), clock=req.clock)
        ticket.run(op)

    def options(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        log.info("[%s] OPTIONS ticket=%s", req.client_addr, ticket_id)

        if ticket_id == "*":
            # Reporting the meta-capabilities for all images.
            allow = ["OPTIONS", "GET", "PUT", "PATCH"]
            features = ["extents", "zero", "flush"]
        else:
            # Reporting real image capabilities per ticket.
            try:
                ticket = auth.authorize(ticket_id, "read")
            except errors.AuthorizationError as e:
                raise http.Error(http.FORBIDDEN, str(e))

            # Accessing ticket options considered as client activity.
            ticket.touch()

            allow = ["OPTIONS"]
            features = ["extents"]
            if ticket.may("read"):
                allow.append("GET")
            if ticket.may("write"):
                allow.extend(("PUT", "PATCH"))
                features.extend(("zero", "flush"))

        resp.headers["allow"] = ",".join(allow)
        msg = {"features": features, "unix_socket": self.config.images.socket}
        resp.send_json(msg)
