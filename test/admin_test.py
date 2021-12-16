# ovirt-imageio
# Copyright (C) 2021 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import uuid

import pytest

from ovirt_imageio import admin
from ovirt_imageio._internal import config
from ovirt_imageio._internal import server

from . import testutil

log = logging.getLogger("test")


@pytest.fixture(scope="module", params=["daemon", "proxy"])
def srv(request):
    path = "test/conf/{}.conf".format(request.param)
    cfg = config.load(path)
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


@pytest.mark.parametrize("trans", ["unix", "tcp"])
def test_client_error(trans):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False, transfer_id="123")

    class cfg:
        class control:
            transport = trans
            socket = "/no/such/sock"
            port = 64 * 1024

    log.debug("Accessing stopped server")
    with pytest.raises(admin.ClientError) as e:
        with admin.Client(cfg, timeout=0.5) as c:
            c.add_ticket(ticket)
    log.debug("Error: %s", e.value)


def test_get_ticket_missing(srv):
    with admin.Client(srv.config) as c:
        ticket_id = str(uuid.uuid4())

        with pytest.raises(admin.ServerError) as e:
            log.debug("Getting ticket %s", ticket_id)
            c.get_ticket(ticket_id)

        log.debug("Error: %s", e.value)
        assert e.value.code == 404
        assert ticket_id in e.value.message


def test_add_ticket(srv):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False, transfer_id="123")

    with admin.Client(srv.config) as c:
        log.debug("Adding ticket %s", ticket)
        c.add_ticket(ticket)
        info = c.get_ticket(ticket["uuid"])
        log.debug("Got ticket %s", info)
        assert info["uuid"] == ticket["uuid"]


def test_get_ticket(srv, fake_time):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False, transfer_id="123")

    with admin.Client(srv.config) as c:
        log.debug("Adding ticket %s", ticket)
        c.add_ticket(ticket)
        info = c.get_ticket(ticket["uuid"])
        log.debug("Got ticket %s", info)

        assert info == {
            # Ticket attributes
            "ops": ticket["ops"],
            "size": ticket["size"],
            "sparse": ticket["sparse"],
            "dirty": ticket["dirty"],
            "inactivity_timeout": ticket["inactivity_timeout"],
            "timeout": ticket["timeout"],
            "url": ticket["url"],
            "uuid": ticket["uuid"],
            "transfer_id": ticket["transfer_id"],
            # Ticket server status.
            "active": False,
            "canceled": False,
            "connections": 0,
            "expires": 300,
            "idle_time": 0,
            "transferred": 0
        }


def test_mod_ticket(srv, fake_time):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False, transfer_id="123")

    with admin.Client(srv.config) as c:
        log.debug("Adding ticket %s", ticket)
        c.add_ticket(ticket)
        info = c.get_ticket(ticket["uuid"])
        assert info["expires"] == 300

        # Extend ticket.
        c.mod_ticket(ticket["uuid"], {"timeout": 600})
        info = c.get_ticket(ticket["uuid"])
        assert info["expires"] == 600

        # Expire ticket.
        c.mod_ticket(ticket["uuid"], {"timeout": 0})
        info = c.get_ticket(ticket["uuid"])
        assert info["expires"] == 0

        # Try to change read only value
        with pytest.raises(admin.ServerError):
            c.mod_ticket(ticket["uuid"], {"url": "file:///other"})

        # Value must not change.
        info = c.get_ticket(ticket["uuid"])
        assert info["url"] == ticket["url"]


def test_del_ticket(srv):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False, transfer_id="123")

    with admin.Client(srv.config) as c:
        log.debug("Adding ticket %s", ticket)
        c.add_ticket(ticket)

        # Make sure ticket exists before removing it.
        c.get_ticket(ticket["uuid"])

        log.debug("Removing ticket %s", ticket["uuid"])
        c.del_ticket(ticket["uuid"])

        with pytest.raises(admin.ServerError) as e:
            c.get_ticket(ticket["uuid"])

        log.debug("Error: %s", e.value)
        assert e.value.code == 404


def test_del_ticket_missing(srv):
    with admin.Client(srv.config) as c:
        ticket_id = str(uuid.uuid4())
        # Shoudld succeed.
        c.del_ticket(ticket_id)
