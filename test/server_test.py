# ovirt-imaged-daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import print_function
from contextlib import closing
from pprint import pprint
import httplib
import json
import os
import ssl
import uuid

import pytest

from ovirt_image_daemon import uhttp
from ovirt_image_daemon import server
from ovirt_image_daemon import util

# Disable client certificate verification introduced in Python > 2.7.9. We
# trust our certificates.
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass  # Older Python, not required


class Config(server.Config):
    host = "127.0.0.1"
    socket = "/tmp/ovirt-image-daemon.sock"
    pki_dir = os.path.join(os.path.dirname(__file__), "pki")
    poll_interval = 0.1


@pytest.fixture(scope="session")
def config(request):
    config = Config()
    server.start(config)
    request.addfinalizer(server.stop)
    return config


def setup_function(f):
    server.tickets.clear()


def test_tickets_no_resource(config):
    res = unix_request(config, "GET", "/no/such/resource")
    assert res.status == 404


def test_tickets_no_method(config):
    res = unix_request(config, "POST", "/tickets/")
    assert res.status == 405


def test_tickets_get(config):
    ticket = create_ticket()
    add_ticket(ticket)
    res = unix_request(config, "GET", "/tickets/%(uuid)s" % ticket)
    assert res.status == 200
    assert json.loads(res.read()) == ticket


def test_tickets_get_not_found(config):
    res = unix_request(config, "GET", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_put(config, monkeypatch):
    monkeypatch.setattr(util, "monotonic_time", lambda: 123456789)
    ticket = create_ticket()
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 200
    ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
    assert server.tickets[ticket["uuid"]] == ticket


def test_tickets_put_invalid_json(config):
    res = unix_request(config, "PUT", "/tickets/", "invalid json")
    assert res.status == 400


def test_tickets_extend(config, monkeypatch):
    now = 123456789
    monkeypatch.setattr(util, "monotonic_time", lambda: now)
    ticket = create_ticket()
    add_ticket(ticket)
    patch = {"timeout": 300}
    body = json.dumps(patch)
    new_ticket = ticket.copy()
    now += 240
    new_ticket["expires"] = now + patch["timeout"]
    res = unix_request(config, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 200
    assert ticket == new_ticket


def test_tickets_extend_no_timeout(config):
    ticket = create_ticket()
    add_ticket(ticket)
    prev_ticket = ticket.copy()
    body = json.dumps({"not-a-timeout": 300})
    res = unix_request(config, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    assert ticket == prev_ticket


def test_tickets_delete_one(config):
    ticket = create_ticket()
    add_ticket(ticket)
    res = unix_request(config, "DELETE", "/tickets/%(uuid)s" % ticket)
    assert res.status == 204
    assert ticket["uuid"] not in server.tickets


def test_tickets_delete_one_not_found(config):
    res = unix_request(config, "DELETE", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_delete_all(config):
    # Example usage: move host to maintenance
    for i in range(5):
        ticket = create_ticket(path="/var/run/vdsm/storage/foo%s" % i)
        add_ticket(ticket)
    res = unix_request(config, "DELETE", "/tickets/")
    assert res.status == 204
    assert server.tickets == {}


def test_images_no_resource(config):
    res = http_request(config, "PUT", "/no/such/resource")
    assert res.status == 404


def test_images_no_method(config):
    res = http_request(config, "POST", "/images/")
    assert res.status == 405


def test_images_upload_no_ticket_id(tmpdir, config):
    payload = create_tempfile(tmpdir, "payload", "content")
    request_id = str(uuid.uuid4())
    res = upload(config, "", request_id, str(payload))
    assert res.status == 400


def test_images_upload_no_ticket(tmpdir, config):
    payload = create_tempfile(tmpdir, "payload", "content")
    ticket_id = str(uuid.uuid4())
    request_id = str(uuid.uuid4())
    res = upload(config, ticket_id, request_id, str(payload))
    assert res.status == 403


def test_images_upload(tmpdir, config):
    payload = create_tempfile(tmpdir, "payload", "content")
    request_id = str(uuid.uuid4())
    image = tmpdir.join("image")
    image.write("-------|after")
    ticket = create_ticket(path=str(image))
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], request_id, str(payload))
    assert image.read() == "content|after"
    assert res.status == 200


@pytest.mark.parametrize("crange,before,after", [
    ("bytes 7-13/20", "before|-------|after", "before|content|after"),
    ("bytes */20", "-------|after", "content|after"),
    ("bytes */*", "-------|after", "content|after"),
])
def test_images_upload_with_range(tmpdir, config, crange, before, after):
    payload = create_tempfile(tmpdir, "payload", "content")
    request_id = str(uuid.uuid4())
    image = tmpdir.join("image")
    image.write(before)
    ticket = create_ticket(path=str(image))
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], request_id, str(payload),
                 content_range=crange)
    assert image.read() == after
    assert res.status == 200


@pytest.mark.parametrize("content_range", [
    "",
    "   ",
    "7-13/20",
    "bytes invalid-invalid/*",
    "bytes 7-13/invalid",
    "bytes 7-13",
    "bytes 13-7/20",
])
def test_images_upload_invalid_range(tmpdir, config, content_range):
    payload = create_tempfile(tmpdir, "payload", "content")
    request_id = str(uuid.uuid4())
    ticket = create_ticket()
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], request_id, str(payload),
                 content_range=content_range)
    assert res.status == 400


def create_ticket(ops=("get", "put"), timeout=300, size=2**64,
                  path="/var/run/vdsm/storage/foo"):
    return {
        "uuid": str(uuid.uuid4()),
        "timeout": timeout,
        "ops": list(ops),
        "size": size,
        "path": path,
    }


def upload(config, ticket_uuid, request_uuid, filename, content_range=None):
    uri = "/images/%s?id=%s" % (ticket_uuid, request_uuid)
    headers = {}
    if content_range is not None:
        headers["content-range"] = content_range
    with open(filename) as f:
        return http_request(config, "PUT", uri, f, headers=headers)


def http_request(config, method, uri, body=None, headers=None):
    con = httplib.HTTPSConnection("127.0.0.1", config.port, config.key_file,
                                  config.cert_file)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def unix_request(config, method, uri, body=None, headers=None):
    con = uhttp.UnixHTTPConnection(config.socket)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason))
    pprint(res.getheaders())
    if res.status >= 400:
        print(res.read())
    return res


def create_tempfile(tmpdir, name, data=''):
    file = tmpdir.join(name)
    file.write(data)
    return file


def create_repo(tmpdir):
    return tmpdir.mkdir("storage")


def create_volume(repo, domain, image, volume, data=''):
    volume = repo.mkdir(domain).mkdir("images").mkdir(image).join(volume)
    volume.write(data)
    return volume


# TODO: move into tickets.py
def add_ticket(ticket):
    ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
    server.tickets[ticket["uuid"]] = ticket
