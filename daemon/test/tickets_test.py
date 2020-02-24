# ovirt-imageio-daemon
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import
from __future__ import print_function

import json
import logging
import os
import ssl
import uuid

from six.moves import http_client

import pytest

from ovirt_imageio import auth
from ovirt_imageio import config
from ovirt_imageio import configloader
from ovirt_imageio import server
from ovirt_imageio import tickets
from ovirt_imageio import util

from . import testutil
from . import http

from . marks import requires_python3

pytestmark = requires_python3

# Disable client certificate verification introduced in Python > 2.7.9. We
# trust our certificates.
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass  # Older Python, not required

logging.basicConfig(
    level=logging.DEBUG,
    format=("%(asctime)s %(levelname)-7s (%(threadName)s) [%(name)s] "
            "%(message)s"))


def setup_module(m):
    conf = os.path.join(os.path.dirname(__file__), "daemon.conf")
    configloader.load(config, [conf])
    server.start(config)


def teardown_module(m):
    server.stop()


def setup_function(f):
    auth.clear()


def test_method_not_allowed():
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("NO_SUCH_METHO", "/tickets/")
        assert res.status == http_client.METHOD_NOT_ALLOWED


def test_no_resource():
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("GET", "/no/such/resource")
        assert res.status == 404


def test_no_method():
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("FOO", "/tickets/")
        assert res.status == 405


def test_get(fake_time):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False, transfer_id="123")
    auth.add(ticket)
    fake_time.now += 200
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("GET", "/tickets/%(uuid)s" % ticket)
        assert res.status == 200
        server_ticket = json.loads(res.read())
        # The server adds an expires key
        del server_ticket["expires"]
        ticket["active"] = False
        ticket["transferred"] = 0
        ticket["idle_time"] = 200
        assert server_ticket == ticket


def test_get_not_found():
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("GET", "/tickets/%s" % uuid.uuid4())
        assert res.status == 404


def test_put(fake_time):
    ticket = testutil.create_ticket(sparse=False, dirty=False)
    body = json.dumps(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PUT", "/tickets/%(uuid)s" % ticket, body)
        # Server adds expires key
        ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
        ticket["active"] = False
        ticket["idle_time"] = 0
        server_ticket = auth.get(ticket["uuid"]).info()
        assert res.status == 200
        assert res.getheader("content-length") == "0"
        assert server_ticket == ticket


def test_put_bad_url_value(fake_time):
    ticket = testutil.create_ticket(url='http://[1.2.3.4:33')
    body = json.dumps(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PUT", "/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_general_exception(monkeypatch):
    def fail(*a, **kw):
        raise Exception("SECRET")
    monkeypatch.setattr(tickets.Handler, "get", fail)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("GET", "/tickets/%s" % uuid.uuid4())
        error = res.read()
        assert res.status == http_client.INTERNAL_SERVER_ERROR
        assert b"SECRET" not in error


def test_put_no_ticket_id():
    ticket = testutil.create_ticket()
    body = json.dumps(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PUT", "/tickets/", body)
        assert res.status == 400
        pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_put_invalid_json():
    ticket = testutil.create_ticket()
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request(
            "PUT",
            "/tickets/%(uuid)s" % ticket,
            "invalid json")
        assert res.status == 400
        pytest.raises(KeyError, auth.get, ticket["uuid"])


# Using "timeout" confuses pytest-timeout plugin, workaround it by using
# "-timeout".
@pytest.mark.parametrize("missing", ["-timeout", "url", "size", "ops"])
def test_put_mandatory_fields(missing):
    ticket = testutil.create_ticket()
    del ticket[missing.strip("-")]
    body = json.dumps(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PUT", "/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_put_invalid_timeout():
    ticket = testutil.create_ticket()
    ticket["timeout"] = "invalid"
    body = json.dumps(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PUT", "/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_put_url_type_error():
    ticket = testutil.create_ticket()
    ticket["url"] = 1
    body = json.dumps(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PUT", "/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_put_url_scheme_not_supported():
    ticket = testutil.create_ticket()
    ticket["url"] = "notsupported:path"
    body = json.dumps(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PUT", "/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400
        pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_extend(fake_time):
    ticket = testutil.create_ticket(sparse=False, dirty=False)
    auth.add(ticket)
    patch = {"timeout": 300}
    body = json.dumps(patch)
    fake_time.now += 240
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/%(uuid)s" % ticket, body)
        ticket["expires"] = int(fake_time.now + ticket["timeout"])
        ticket["active"] = False
        ticket["idle_time"] = 240
        server_ticket = auth.get(ticket["uuid"]).info()
        assert res.status == 200
        assert res.getheader("content-length") == "0"
        assert server_ticket == ticket


def test_extend_negative_timeout():
    ticket = testutil.create_ticket(sparse=False)
    auth.add(ticket)
    patch = {"timeout": -1}
    body = json.dumps(patch)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/%(uuid)s" % ticket, body)
        assert res.status == 400


def test_get_expired_ticket(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("GET", "/tickets/%(uuid)s" % ticket)
        assert res.status == 200


def test_extend_expired_ticket(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    server_ticket = auth.get(ticket["uuid"]).info()
    # Extend the expired ticket.
    body = json.dumps({"timeout": 300})
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/%(uuid)s" % ticket, body)
        assert res.status == 200
        server_ticket = auth.get(ticket["uuid"]).info()
        assert server_ticket["expires"] == 800


def test_extend_no_ticket_id(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    body = json.dumps({"timeout": 300})
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/", body)
        cur_ticket = auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_invalid_json(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/%(uuid)s" % ticket, "{invalid}")
        cur_ticket = auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_no_timeout(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    body = json.dumps({"not-a-timeout": 300})
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/%(uuid)s" % ticket, body)
        cur_ticket = auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_invalid_timeout(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    body = json.dumps({"timeout": "invalid"})
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/%(uuid)s" % ticket, body)
        cur_ticket = auth.get(ticket["uuid"]).info()
        assert res.status == 400
        assert cur_ticket == prev_ticket


def test_extend_not_found():
    ticket_id = str(uuid.uuid4())
    body = json.dumps({"timeout": 300})
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("PATCH", "/tickets/%s" % ticket_id, body)
        assert res.status == 404


def test_idle_time_active(fake_time, tmpdir):
    filename = tmpdir.join("image")
    # Note: must be big enough so the request remain active.
    size = 1024**2 * 10
    with open(str(filename), 'wb') as image:
        image.truncate(size)
    ticket = testutil.create_ticket(
        url="file://" + str(filename), ops=["read"], size=size)
    auth.add(ticket)

    # Start a download, but read only 1 byte to make sure the operation becomes
    # active but do not complete.
    with http.Client(config) as c:
        res = c.get("/images/" + ticket["uuid"])
        res.read(1)

        # Active ticket idle time is always 0.
        fake_time.now += 200
        assert auth.get(ticket["uuid"]).idle_time == 0


def test_idle_time_inactive(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)

    # Ticket idle time starts with ticket is added.
    assert auth.get(ticket["uuid"]).idle_time == 0

    # Simulate time passing without any request.
    fake_time.now += 200
    assert auth.get(ticket["uuid"]).idle_time == 200


def test_idle_time_put(fake_time, tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    with http.Client(config) as c:
        c.put("/images/" + ticket["uuid"], "b" * 8192)
        assert auth.get(ticket["uuid"]).idle_time == 0


def test_idle_time_get(fake_time, tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    with http.Client(config) as c:
        c.get("/images/" + ticket["uuid"])
        assert auth.get(ticket["uuid"]).idle_time == 0


@pytest.mark.parametrize("msg", [
    pytest.param({"op": "zero", "size": 1}, id="zero"),
    pytest.param({"op": "flush"}, id="flush"),
])
def test_idle_time_patch(fake_time, tmpdir, msg):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    body = json.dumps(msg).encode('ascii')

    with http.Client(config) as c:
        c.patch("/images/" + ticket["uuid"], body,
                headers={"content-type": "application/json"})
        assert auth.get(ticket["uuid"]).idle_time == 0


def test_idle_time_options(fake_time):
    ticket = testutil.create_ticket(url="file:///no/such/file")
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    with http.Client(config) as c:
        c.options("/images/" + ticket["uuid"])
        assert auth.get(ticket["uuid"]).idle_time == 0


def test_delete_one():
    ticket = testutil.create_ticket()
    auth.add(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("DELETE", "/tickets/%(uuid)s" % ticket)
        assert res.status == 204
        # Note: incorrect according to RFC, but required for vdsm.
        assert res.getheader("content-length") == "0"
        pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_delete_one_not_found():
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("DELETE", "/tickets/no-such-ticket")
        assert res.status == 204
        # Note: incorrect according to RFC, but required for vdsm.
        assert res.getheader("content-length") == "0"


def test_delete_all():
    # Example usage: move host to maintenance
    for i in range(5):
        ticket = testutil.create_ticket(
            url="file:///var/run/vdsm/storage/foo%s" % i)
        auth.add(ticket)
    with http.UnixClient(config.tickets.socket) as c:
        res = c.request("DELETE", "/tickets/")
        assert res.status == 204
        # Note: incorrect according to RFC, but required for vdsm.
        assert res.getheader("content-length") == "0"
        pytest.raises(KeyError, auth.get, ticket["uuid"])
