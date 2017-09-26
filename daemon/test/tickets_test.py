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

    def __init__(self, active=True, offset=0, done=0):
        self.active = active
        self.offset = offset
        self.done = done


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
        ticket = Ticket(ticket_dict=testutils.create_ticket())
        for op in operations:
            ticket.add_operation(op)
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
        ticket = Ticket(ticket_dict=testutils.create_ticket(ops=["read"]))
        for offset, done in operations:
            ticket.add_operation(FakeOperation(offset=offset, done=done))
        assert ticket.transferred() == transferred

    @pytest.mark.parametrize("transferred_gb", [1, 8, 64, 512, 4096])
    def test_benchmark_transferred(self, transferred_gb):
        ticket = Ticket(ticket_dict=testutils.create_ticket(ops=["read"]))
        operations = transferred_gb * 1024**3 // CHUNK_SIZE
        for i in range(operations):
            ticket.add_operation(FakeOperation(offset=i * CHUNK_SIZE,
                                               done=CHUNK_SIZE))
        start = time.time()
        assert ticket.transferred() == transferred_gb * 1024**3
        end = time.time()
        print("%dG file (%d operations) in %.6f seconds" %
              (transferred_gb, operations, end - start))
