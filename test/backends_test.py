# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from urllib.parse import urlunparse
import pytest

from ovirt_imageio._internal import auth
from ovirt_imageio._internal import backends
from ovirt_imageio._internal import config
from ovirt_imageio._internal import errors
from ovirt_imageio._internal import nbd

from . import testutil
from . marks import flaky_in_ovirt_ci


class Request:

    connection_id = 1

    def __init__(self):
        self.context = {}
        self.connection_timeout = None

    def set_connection_timeout(self, timeout):
        self.connection_timeout = timeout


@pytest.fixture
def cfg():
    return config.load([])


def test_get_caching(tmpurl, cfg):
    ticket = auth.Ticket(
        testutil.create_ticket(url=urlunparse(tmpurl)), cfg)
    req = Request()
    c1 = backends.get(req, ticket, cfg)

    # Context is cached in the ticket.
    assert ticket.get_context(req.connection_id) is c1
    assert c1.backend.name == "file"
    assert len(c1.buffer) == cfg.backend_file.buffer_size

    # Next call return the cached instance.
    c2 = backends.get(req, ticket, cfg)
    assert c1 is c2

    # Closing req.context removes the context from the ticket.
    req.context[ticket.uuid].close()

    c3 = backends.get(req, ticket, cfg)
    assert c2.backend.name == "file"
    assert c3 is not c1


def test_get_set_timeout(tmpurl, cfg):
    ticket = auth.Ticket(
        testutil.create_ticket(
            url=urlunparse(tmpurl),
            inactivity_timeout=300),
        cfg)

    req = Request()
    assert req.connection_timeout is None

    # Authorizing the ticket set connection timeout.
    backends.get(req, ticket, cfg)
    assert req.connection_timeout == ticket.inactivity_timeout


def test_get_canceled_ticket(tmpurl, cfg):
    ticket = auth.Ticket(
        testutil.create_ticket(url=urlunparse(tmpurl)), cfg)
    req = Request()
    ticket.cancel()

    # If the ticket was canceled, getting a backend raises.
    with pytest.raises(errors.AuthorizationError):
        backends.get(req, ticket, cfg)

    # And nothing is stored in the request context.
    assert req.context == {}


@pytest.mark.parametrize("ops,readable,writable", [
    (["read"], True, False),
    (["read", "write"], True, True),
    (["write"], True, True),
])
def test_get_ops(tmpurl, cfg, ops, readable, writable):
    ticket = auth.Ticket(
        testutil.create_ticket(url=urlunparse(tmpurl), ops=ops), cfg)
    req = Request()
    b = backends.get(req, ticket, cfg).backend

    # Create a read-write file backend.
    assert b.name == "file"
    assert b.readable() == readable
    assert b.writable() == writable


@pytest.mark.parametrize("sparse", [True, False])
def test_get_sparse(tmpurl, cfg, sparse):
    ticket = auth.Ticket(
        testutil.create_ticket(url=urlunparse(tmpurl), sparse=sparse), cfg)
    req = Request()
    b = backends.get(req, ticket, cfg).backend

    assert b.name == "file"
    assert b.sparse == sparse


@pytest.mark.parametrize("transport", [
    "unix",
    pytest.param("tcp", marks=flaky_in_ovirt_ci),
])
def test_get_nbd_backend(tmpdir, cfg, nbd_server, transport):
    if transport == "unix":
        nbd_server.sock = nbd.UnixAddress(tmpdir.join("sock"))
    else:
        nbd_server.sock = nbd.TCPAddress(
            "localhost", testutil.random_tcp_port())
    nbd_server.start()

    ticket = auth.Ticket(
        testutil.create_ticket(url=urlunparse(nbd_server.url)), cfg)
    req = Request()
    b = backends.get(req, ticket, cfg).backend

    assert b.name == "nbd"
