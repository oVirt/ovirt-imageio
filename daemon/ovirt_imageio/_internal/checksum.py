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

        with req.clock.run("checksum"):
            checksum = compute(ctx.backend, ctx.buffer, algorithm=algorithm)

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


def compute(backend, buf, algorithm="sha1"):
    """
    Compute image checksum.
    """
    h = Hasher(algorithm)
    for extent in backend.extents("zero"):
        if extent.data:
            backend.seek(extent.start)
            h.read_from(backend, extent.length, buf)
        else:
            h.zero(extent.length)
    return h.hexdigest()


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
