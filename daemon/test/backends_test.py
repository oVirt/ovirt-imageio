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

from ovirt_imageio._internal import backends
from ovirt_imageio._internal import config
from ovirt_imageio._internal import nbd

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


@pytest.fixture
def cfg():
    return config.load([])


def test_get_unsupported_scheme(cfg):
    ticket = Ticket("test", urlparse("unsupported:/path"))
    req = Request()
    with pytest.raises(backends.Unsupported):
        backends.get(req, ticket, cfg)


def test_get_caching(tmpurl, cfg):
    ticket = Ticket("test", tmpurl)
    req = Request()
    c1 = backends.get(req, ticket, cfg)

    # Backend and buffer are cached in req.context.
    assert req.context[ticket.uuid] is c1
    assert c1.backend.name == "file"
    assert len(c1.buffer) == cfg.daemon.buffer_size

    # Next call return the cached instance.
    c2 = backends.get(req, ticket, cfg)
    assert c1 is c2

    # Deleting the cache creates a new instance.
    del req.context[ticket.uuid]
    c3 = backends.get(req, ticket, cfg)
    assert c2.backend.name == "file"
    assert c3 is not c1


@pytest.mark.parametrize("ops,readable,writable", [
    (["read"], True, False),
    (["read", "write"], True, True),
    (["write"], True, True),
])
def test_get_ops(tmpurl, cfg, ops, readable, writable):
    ticket = Ticket("test", tmpurl, ops=ops)
    req = Request()
    b = backends.get(req, ticket, cfg).backend

    # Create a read-write file backend.
    assert b.name == "file"
    assert b.readable() == readable
    assert b.writable() == writable


@pytest.mark.parametrize("sparse", [True, False])
def test_get_sparse(tmpurl, cfg, sparse):
    ticket = Ticket("test", tmpurl, sparse=sparse)
    req = Request()
    b = backends.get(req, ticket, cfg).backend

    assert b.name == "file"
    assert b.sparse == sparse


@requires_python3
@pytest.mark.parametrize("transport", ["unix", "tcp"])
def test_get_nbd_backend(tmpdir, cfg, nbd_server, transport):
    if transport == "unix":
        nbd_server.sock = nbd.UnixAddress(tmpdir.join("sock"))
    else:
        nbd_server.sock = nbd.TCPAddress(
            "localhost", testutil.random_tcp_port())
    nbd_server.start()

    ticket = Ticket("test", nbd_server.url)
    req = Request()
    b = backends.get(req, ticket, cfg).backend

    assert b.name == "nbd"
