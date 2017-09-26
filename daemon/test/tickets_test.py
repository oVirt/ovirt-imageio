# ovirt-imageio-daemon
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest

from test import testutils
from ovirt_imageio_daemon.tickets import Ticket


class FakeOperation(object):
    """
    Used to fake a directio.Operation object.
    """

    def __init__(self, active=True):
        self.active = active


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
