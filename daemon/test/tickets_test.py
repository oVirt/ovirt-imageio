# ovirt-imageio-daemon
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest
import time

from ovirt_imageio_daemon.tickets import Ticket
from test import testutils

CHUNK_SIZE = 8 * 1024**2


class FakeOperation(object):
    """
    Used to fake a directio.Operation object.
    """

    def __init__(self, active=True, offset=0, done=0, data=()):
        self.active = active
        self.offset = offset
        self.done = done
        self.data = data
        self.was_run = False

    def __iter__(self):
        return iter(self.data)

    def run(self):
        self.was_run = True
        self.active = False

    def close(self):
        self.active = False


class TestTicket(object):

    @pytest.mark.parametrize("operations,active", [
        ([], False),
        ([FakeOperation(active=True)], True),
        ([FakeOperation(active=True), FakeOperation(active=True)], True),
        ([FakeOperation(active=False)], False),
        ([FakeOperation(active=False), FakeOperation(active=False)], False),
        ([FakeOperation(active=True), FakeOperation(active=False)], True),
    ])
    def test_active(self, operations, active):
        ticket = Ticket(testutils.create_ticket())
        ticket._operations = operations
        assert ticket.active() == active

    @pytest.mark.parametrize("operations,transferred", [
        ([], 0),
        ([(0, 0)], 0),
        ([(0, 0), (10, 0)], 0),
        ([(0, 0), (0, 0)], 0),
        ([(0, 10), (10, 10)], 20),
        ([(10, 10), (0, 5)], 15),
        ([(10, 10), (0, 15), (18, 7)], 25),
    ])
    def test_transferred(self, operations, transferred):
        ticket = Ticket(testutils.create_ticket(ops=["read"]))
        ticket._operations = [FakeOperation(offset=offset, done=done)
                              for offset, done in operations]
        assert ticket.transferred() == transferred

    @pytest.mark.benchmark
    @pytest.mark.parametrize("transferred_gb", [1, 8, 64, 512, 4096])
    def test_benchmark_transferred(self, transferred_gb):
        ticket = Ticket(testutils.create_ticket(ops=["read"]))
        operations = transferred_gb * 1024**3 // CHUNK_SIZE
        ticket._operations = [
            FakeOperation(offset=i * CHUNK_SIZE, done=CHUNK_SIZE)
            for i in range(operations)]
        start = time.time()
        assert ticket.transferred() == transferred_gb * 1024**3
        end = time.time()
        print("%dG file (%d operations) in %.6f seconds" %
              (transferred_gb, operations, end - start))

    def test_repr(self):
        ticket = Ticket(testutils.create_ticket(
            ops=["read"], filename="tmp_file"))
        ticket_repr = repr(ticket)

        info = ticket.info()
        del info["timeout"]

        for key, value in info.items():
            pair = "%s=%r" % (key, value)
            assert pair in ticket_repr


def test_ticket_run():
    ticket = Ticket(testutils.create_ticket())
    op = FakeOperation()
    ticket.run(op)
    assert op.was_run
    assert op in ticket._operations


def test_ticket_bind():
    ticket = Ticket(testutils.create_ticket())
    op = FakeOperation(data=["chunk 1", "chunk 2", "chunk 3"])
    bop = ticket.bind(op)
    assert op in ticket._operations

    # Use as WebOB.Response.app_iter.
    data = list(bop)
    bop.close()
    assert data == op.data
    assert not op.active
