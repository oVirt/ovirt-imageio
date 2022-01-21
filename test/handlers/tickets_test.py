# ovirt-imageio
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import http.client as http_client
import json
import uuid

import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import server
from ovirt_imageio._internal import util
from ovirt_imageio._internal.handlers import tickets

from .. import testutil
from .. import http


@pytest.fixture(
    scope="module",
    params=["daemon", "proxy"]
)
def srv(request):
    path = "test/conf/{}.conf".format(request.param)
    cfg = config.load(path)
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


def test_method_not_allowed(srv):
    with http.ControlClient(srv.config) as c:
        res = c.request("NO_SUCH_METHO", "/tickets/")
        assert res.status == http_client.METHOD_NOT_ALLOWED


def test_no_resource(srv):
    with http.ControlClient(srv.config) as c:
        res = c.get("/no/such/resource")
        assert res.status == 404


def test_no_method(srv):
    with http.ControlClient(srv.config) as c:
        res = c.request("FOO", "/tickets/")
        assert res.status == 405


def test_get(srv, fake_time):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False)
    srv.auth.add(ticket)
    fake_time.now += 200
    with http.ControlClient(srv.config) as c:
        res = c.get("/tickets/%(uuid)s" % ticket)
        assert res.status == 200
        server_ticket = json.loads(res.read())
        # The server adds an expires key
        del server_ticket["expires"]
        ticket["active"] = False
        ticket["transferred"] = 0
        ticket["idle_time"] = 200
        ticket["canceled"] = False
        ticket["connections"] = 0
        assert server_ticket == ticket


def test_get_not_found(srv):
    with http.ControlClient(srv.config) as c:
        res = c.get("/tickets/%s" % uuid.uuid4())
        assert res.status == 404


def test_put(srv, fake_time):
    ticket = testutil.create_ticket(sparse=False, dirty=False)
    body = json.dumps(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/%(uuid)s" % ticket, body)
        # Server adds expires key
        ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
        ticket["active"] = False
        ticket["idle_time"] = 0
        ticket["canceled"] = False
        ticket["connections"] = 0
        server_ticket = srv.auth.get(ticket["uuid"]).info()
        assert res.status == 200
        assert res.getheader("content-length") == "0"
        assert server_ticket == ticket


def test_put_bad_url_value(srv, fake_time):
    ticket = testutil.create_ticket(url='http://[1.2.3.4:33')
    body = json.dumps(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


def test_general_exception(srv, monkeypatch):
    def fail(*a, **kw):
        raise Exception("SECRET")
    monkeypatch.setattr(tickets.Handler, "get", fail)
    with http.ControlClient(srv.config) as c:
        res = c.get("/tickets/%s" % uuid.uuid4())
        error = res.read()
        assert res.status == http_client.INTERNAL_SERVER_ERROR
        assert b"SECRET" not in error


def test_put_no_ticket_id(srv):
    ticket = testutil.create_ticket()
    body = json.dumps(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/", body)
        assert res.status == 400
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


def test_put_invalid_json(srv):
    ticket = testutil.create_ticket()
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/%(uuid)s" % ticket, "invalid json")
        assert res.status == 400
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


# Using "timeout" confuses pytest-timeout plugin, workaround it by using
# "-timeout".
@pytest.mark.parametrize("missing", ["-timeout", "url", "size", "ops"])
def test_put_mandatory_fields(srv, missing):
    ticket = testutil.create_ticket()
    del ticket[missing.strip("-")]
    body = json.dumps(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


def test_put_invalid_timeout(srv):
    ticket = testutil.create_ticket()
    ticket["timeout"] = "invalid"
    body = json.dumps(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


def test_put_url_type_error(srv):
    ticket = testutil.create_ticket()
    ticket["url"] = 1
    body = json.dumps(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


def test_put_url_scheme_not_supported(srv):
    ticket = testutil.create_ticket()
    ticket["url"] = "notsupported:path"
    body = json.dumps(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.put("/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


def test_extend(srv, fake_time):
    ticket = testutil.create_ticket(sparse=False, dirty=False)
    srv.auth.add(ticket)
    patch = {"timeout": 300}
    body = json.dumps(patch)
    fake_time.now += 240
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/%(uuid)s" % ticket, body)
        ticket["expires"] = int(fake_time.now + ticket["timeout"])
        ticket["active"] = False
        ticket["idle_time"] = 240
        ticket["canceled"] = False
        ticket["connections"] = 0
        server_ticket = srv.auth.get(ticket["uuid"]).info()
        assert res.status == 200
        assert res.getheader("content-length") == "0"
        assert server_ticket == ticket


def test_extend_negative_timeout(srv):
    ticket = testutil.create_ticket(sparse=False)
    srv.auth.add(ticket)
    patch = {"timeout": -1}
    body = json.dumps(patch)
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400


def test_get_expired_ticket(srv, fake_time):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    with http.ControlClient(srv.config) as c:
        res = c.get("/tickets/%(uuid)s" % ticket)
        assert res.status == 200


def test_extend_expired_ticket(srv, fake_time):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    server_ticket = srv.auth.get(ticket["uuid"]).info()
    # Extend the expired ticket.
    body = json.dumps({"timeout": 300})
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/%(uuid)s" % ticket, body)
        assert res.status == 200
        server_ticket = srv.auth.get(ticket["uuid"]).info()
        assert server_ticket["expires"] == 800


def test_extend_no_ticket_id(srv, fake_time):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    prev_ticket = srv.auth.get(ticket["uuid"]).info()
    body = json.dumps({"timeout": 300})
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/", body)
        cur_ticket = srv.auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_invalid_json(srv, fake_time):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    prev_ticket = srv.auth.get(ticket["uuid"]).info()
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/%(uuid)s" % ticket, "{invalid}")
        cur_ticket = srv.auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_no_timeout(srv, fake_time):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    prev_ticket = srv.auth.get(ticket["uuid"]).info()
    body = json.dumps({"not-a-timeout": 300})
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/%(uuid)s" % ticket, body)
        cur_ticket = srv.auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_invalid_timeout(srv, fake_time):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    prev_ticket = srv.auth.get(ticket["uuid"]).info()
    body = json.dumps({"timeout": "invalid"})
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/%(uuid)s" % ticket, body)
        cur_ticket = srv.auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_not_found(srv):
    ticket_id = str(uuid.uuid4())
    body = json.dumps({"timeout": 300})
    with http.ControlClient(srv.config) as c:
        res = c.patch("/tickets/%s" % ticket_id, body)
        assert res.status == 404


def test_idle_time_active(srv, fake_time, tmpdir):
    filename = tmpdir.join("image")
    # Note: must be big enough so the request remain active.
    size = 1024**2 * 10
    with open(str(filename), 'wb') as image:
        image.truncate(size)
    ticket = testutil.create_ticket(
        url="file://" + str(filename), ops=["read"], size=size)
    srv.auth.add(ticket)

    # Start a download, but read only 1 byte to make sure the operation becomes
    # active but do not complete.
    with http.RemoteClient(srv.config) as c:
        res = c.get("/images/" + ticket["uuid"])
        res.read(1)

        # Active ticket idle time is always 0.
        fake_time.now += 200
        assert srv.auth.get(ticket["uuid"]).idle_time == 0


def test_idle_time_inactive(srv, fake_time):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)

    # Ticket idle time starts with ticket is added.
    assert srv.auth.get(ticket["uuid"]).idle_time == 0

    # Simulate time passing without any request.
    fake_time.now += 200
    assert srv.auth.get(ticket["uuid"]).idle_time == 200


def test_idle_time_put(srv, fake_time, tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    with http.RemoteClient(srv.config) as c:
        c.put("/images/" + ticket["uuid"], "b" * 8192)
        assert srv.auth.get(ticket["uuid"]).idle_time == 0


def test_idle_time_get(srv, fake_time, tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    with http.RemoteClient(srv.config) as c:
        c.get("/images/" + ticket["uuid"])
        assert srv.auth.get(ticket["uuid"]).idle_time == 0


@pytest.mark.parametrize("msg", [
    pytest.param({"op": "zero", "size": 1}, id="zero"),
    pytest.param({"op": "flush"}, id="flush"),
])
def test_idle_time_patch(srv, fake_time, tmpdir, msg):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    body = json.dumps(msg).encode('ascii')

    with http.RemoteClient(srv.config) as c:
        c.patch("/images/" + ticket["uuid"], body,
                headers={"content-type": "application/json"})
        assert srv.auth.get(ticket["uuid"]).idle_time == 0


def test_idle_time_options(srv, tmpdir, fake_time):
    image = testutil.create_tempfile(tmpdir, "image", size=8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    with http.RemoteClient(srv.config) as c:
        c.options("/images/" + ticket["uuid"])
        assert srv.auth.get(ticket["uuid"]).idle_time == 0


def test_delete_one(srv):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.delete("/tickets/%(uuid)s" % ticket)
        assert res.status == 204
        # Note: incorrect according to RFC, but required for vdsm.
        assert res.getheader("content-length") == "0"
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])


def test_delete_one_not_found(srv):
    with http.ControlClient(srv.config) as c:
        res = c.delete("/tickets/no-such-ticket")
        assert res.status == 204
        # Note: incorrect according to RFC, but required for vdsm.
        assert res.getheader("content-length") == "0"


def test_delete_all(srv):
    # Example usage: move host to maintenance
    for i in range(5):
        ticket = testutil.create_ticket(
            url="file:///tmp/foo%s" % i)
        srv.auth.add(ticket)
    with http.ControlClient(srv.config) as c:
        res = c.delete("/tickets/")
        assert res.status == 204
        # Note: incorrect according to RFC, but required for vdsm.
        assert res.getheader("content-length") == "0"
        pytest.raises(KeyError, srv.auth.get, ticket["uuid"])
