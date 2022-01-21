# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import hashlib
import logging

from .. import backends
from .. import blkhash
from .. import errors
from .. import http
from .. import ioutil
from .. import ops
from .. import util
from .. import validate

log = logging.getLogger("checksum")

ALGORITHMS = frozenset(hashlib.algorithms_available)

# Limit allowed block size to avoid abusing server resources.
MIN_BLOCK_SIZE = blkhash.BLOCK_SIZE // 4
MAX_BLOCK_SIZE = blkhash.BLOCK_SIZE * 4


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

        algorithm = validate.enum(
            req.query,
            "algorithm",
            ALGORITHMS,
            default=blkhash.ALGORITHM)

        try:
            block_size = int(req.query.get("block_size", blkhash.BLOCK_SIZE))
        except ValueError:
            raise http.Error(
                http.BAD_REQUEST,
                "Invalid block size: {!r}".format(req.query["block_size"]))

        if not MIN_BLOCK_SIZE <= block_size <= MAX_BLOCK_SIZE:
            raise http.Error(
                http.BAD_REQUEST,
                "Block size out of allowed range: {}-{}"
                .format(MIN_BLOCK_SIZE, MAX_BLOCK_SIZE))

        if block_size % 4096:
            raise http.Error(
                http.BAD_REQUEST, "Block size is not aligned to 4096")

        try:
            ticket = self.auth.authorize(ticket_id, "read")
            ctx = backends.get(req, ticket, self.config)
        except errors.AuthorizationError as e:
            raise http.Error(http.FORBIDDEN, str(e))

        log.info("[%s] CHECKSUM transfer=%s algorithm=%s block_size=%s",
                 req.client_addr, ticket.transfer_id, algorithm, block_size)

        # For simplicity we create a new buffer even if block_size is same as
        # ctx.buffer length.

        with util.aligned_buffer(block_size) as buf:
            op = Operation(ctx.backend, buf, algorithm, clock=req.clock)
            try:
                checksum = ticket.run(op)
            except errors.AuthorizationError as e:
                resp.close_connection()
                raise http.Error(http.FORBIDDEN, str(e)) from None

        resp.send_json(checksum)


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

    def __init__(self, backend, buf, algorithm, detect_zeroes=True,
                 clock=None):
        super().__init__(size=backend.size(), buf=buf, clock=clock)
        self._backend = backend
        self._algorithm = algorithm
        self._detect_zeroes = detect_zeroes

    def _run(self):
        block_size = len(self._buf)
        # Only blakse2b and blake2s support variable digest size, and 32 works
        # with both and is large enough.
        if self._algorithm.startswith("blake2"):
            digest_size = blkhash.DIGEST_SIZE
        else:
            digest_size = None

        h = blkhash.Hash(
            block_size=block_size,
            algorithm=self._algorithm,
            digest_size=digest_size)

        for block in blkhash.split(self._backend.extents("zero"), block_size):
            if block.zero:
                h.zero(block.length)
            else:
                with memoryview(self._buf)[:block.length] as view:
                    self._backend.seek(block.start)
                    self._backend.readinto(view)
                    if self._detect_zeroes and ioutil.is_zero(view):
                        h.zero(block.length)
                    else:
                        h.update(view)

            self._done += block.length

            if self._canceled:
                raise ops.Canceled

        return {
            "algorithm": self._algorithm,
            "block_size": block_size,
            "checksum": h.hexdigest(),
        }


def compute(backend, buf, algorithm=blkhash.ALGORITHM, detect_zeroes=True):
    """
    Compute image checksum.
    """
    op = Operation(backend, buf, algorithm, detect_zeroes=detect_zeroes)
    return op.run()
