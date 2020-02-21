# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from six.moves.urllib_parse import urlparse
import pytest

from ovirt_imageio import nbd
from ovirt_imageio import backends

from . import testutil
from . marks import requires_python3


class Ticket(object):

    def __init__(self, uuid, url, ops=("read",), sparse=False, dirty=False):
        self.uuid = uuid
        self.url = url
        self.ops = ops
        self.sparse = sparse
        self.dirty = dirty


class Request(object):

    def __init__(self):
        self.context = {}


def test_get_unsupported_scheme():
    ticket = Ticket("test", urlparse("unsupported:/path"))
    req = Request()
    with pytest.raises(backends.Unsupported):
        backends.get(req, ticket)


def test_get_caching(tmpurl):
    ticket = Ticket("test", tmpurl)
    req = Request()
    b1 = backends.get(req, ticket)

    # Backend is cache in req.context.
    assert b1.name == "file"
    assert req.context[ticket.uuid] is b1

    # Next call return the cached instance.
    b2 = backends.get(req, ticket)
    assert b1 is b2

    # Deleting the cache creates a new instance.
    del req.context[ticket.uuid]
    b3 = backends.get(req, ticket)
    assert b3.name == "file"
    assert b3 is not b1


@pytest.mark.parametrize("ops,readable,writable", [
    (["read"], True, False),
    (["read", "write"], True, True),
    (["write"], True, True),
])
def test_get_ops(tmpurl, ops, readable, writable):
    ticket = Ticket("test", tmpurl, ops=ops)
    req = Request()
    b = backends.get(req, ticket)

    # Create a read-write file backend.
    assert b.name == "file"
    assert b.readable() == readable
    assert b.writable() == writable


@pytest.mark.parametrize("sparse", [True, False])
def test_get_sparse(tmpurl, sparse):
    ticket = Ticket("test", tmpurl, sparse=sparse)
    req = Request()
    b = backends.get(req, ticket)

    assert b.name == "file"
    assert b.sparse == sparse


@requires_python3
@pytest.mark.parametrize("transport", ["unix", "tcp"])
def test_get_nbd_backend(tmpdir, nbd_server, transport):
    if transport == "unix":
        nbd_server.sock = nbd.UnixAddress(tmpdir.join("sock"))
    else:
        nbd_server.sock = nbd.TCPAddress(
            "localhost", testutil.random_tcp_port())
    nbd_server.start()

    ticket = Ticket("test", nbd_server.url)
    req = Request()
    b = backends.get(req, ticket)

    assert b.name == "nbd"
