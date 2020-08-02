# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import hashlib
import logging

from . import backends
from . import errors
from . import http
from . import ops
from . import validate

log = logging.getLogger("checksum")

ALGORITHMS = frozenset(hashlib.algorithms_available)


class Checksum:
    """
    Handle requests for the /images/ticket-id/checksum resource.
    """

    def __init__(self, config, auth):
        self.config = config
        self.auth = auth

    def get(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        try:
            ticket = self.auth.authorize(ticket_id, "read")
        except errors.AuthorizationError as e:
            raise http.Error(http.FORBIDDEN, str(e))

        algorithm = validate.enum(
            req.query,
            "algorithm",
            ALGORITHMS,
            default="sha1")

        log.info("[%s] CHECKSUM ticket=%s algorithm=%s",
                 req.client_addr, ticket_id, algorithm)

        ctx = backends.get(req, ticket, self.config)

        op = Operation(ctx.backend, ctx.buffer, algorithm, clock=req.clock)
        try:
            checksum = ticket.run(op)
        except errors.AuthorizationError as e:
            resp.close_connection()
            raise http.Error(http.FORBIDDEN, str(e)) from None

        resp.send_json({"checksum": checksum, "algorithm": algorithm})


class Algorithms:
    """
    Handle requests for the /images/ticket-id/checksum/algorithms resource.
    """

    def __init__(self, config, auth):
        self.config = config
        self.auth = auth

    def get(self, req, resp, *args):
        # Server information, no authorization needed.
        resp.send_json({"algorithms": sorted(ALGORITHMS)})


class Operation(ops.Operation):
    """
    Checksum operation.
    """

    name = "checksum"

    def __init__(self, backend, buf, algorithm, clock=None):
        super().__init__(size=backend.size(), buf=buf, clock=clock)
        self._backend = backend
        self._algorithm = algorithm

    def _run(self):
        h = Hasher(self._algorithm)

        # TODO: Split big extents to have progress for preallocated or empty
        # images.
        for extent in self._backend.extents("zero"):
            if extent.data:
                self._backend.seek(extent.start)
                with self._record("read_from") as s:
                    h.read_from(self._backend, extent.length, self._buf)
                    s.bytes += extent.length
            else:
                with self._record("zero") as s:
                    h.zero(extent.length)
                    s.bytes += extent.length

            self._done += extent.length

            if self._canceled:
                raise ops.Canceled

        return h.hexdigest()


def compute(backend, buf, algorithm="sha1"):
    """
    Compute image checksum.
    """
    op = Operation(backend, buf, algorithm)
    return op.run()


class Hasher:

    def __init__(self, algorithm):
        self._hash = hashlib.new(algorithm)

    def read_from(self, reader, length, buf):
        todo = length
        max_step = len(buf)

        with memoryview(buf) as view:
            while todo:
                step = min(todo, max_step)
                n = reader.readinto(view[:step])
                if n == 0:
                    raise RuntimeError(
                        "Expected {} bytes, got {} bytes"
                        .format(length, length - todo))

                self._hash.update(view[:n])
                todo -= n

    def zero(self, count):
        step = min(64 * 1024, count)
        buf = bytearray(step)

        while count > step:
            self._hash.update(buf)
            count -= step

        with memoryview(buf)[:count] as v:
            self._hash.update(v)

    def hexdigest(self):
        return self._hash.hexdigest()
