# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import print_function
from contextlib import contextmanager, closing
from pprint import pprint
import httplib
import json
import os
import time
import uuid

from imaged import uhttp
from imaged import server


def setup_function(f):
    server.tickets.clear()


def test_tickets_no_resource():
    config = Config()
    with run_imaged(config):
        res = unix_request(config, "GET", "/no/such/resource")
    assert res.status == 404


def test_tickets_no_method():
    config = Config()
    with run_imaged(config):
        res = unix_request(config, "POST", "/tickets/")
    assert res.status == 405


def test_tickets_get():
    config = Config()
    ticket = create_ticket()
    server.tickets[ticket["uuid"]] = ticket
    with run_imaged(config):
        res = unix_request(config, "GET", "/tickets/%(uuid)s" % ticket)
    assert res.status == 200
    assert json.loads(res.read()) == ticket


def test_tickets_get_not_found():
    config = Config()
    with run_imaged(config):
        res = unix_request(config, "GET", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_put():
    config = Config()
    ticket = create_ticket()
    body = json.dumps(ticket)
    with run_imaged(config):
        res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 200
    assert server.tickets[ticket["uuid"]] == ticket


def test_tickets_put_invalid_json():
    config = Config()
    with run_imaged(config):
        res = unix_request(config, "PUT", "/tickets/", "invalid json")
    assert res.status == 400


def test_tickets_extend():
    config = Config()
    ticket = create_ticket()
    server.tickets[ticket["uuid"]] = ticket
    patch = {"expires": ticket["expires"] + 300}
    body = json.dumps(patch)
    with run_imaged(config):
        res = unix_request(config, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 200
    assert ticket["expires"] == patch["expires"]


def test_tickets_delete_one():
    config = Config()
    ticket = create_ticket()
    server.tickets[ticket["uuid"]] = ticket
    with run_imaged(config):
        res = unix_request(config, "DELETE", "/tickets/%(uuid)s" % ticket)
    assert res.status == 204
    assert ticket["uuid"] not in server.tickets


def test_tickets_delete_one_not_found():
    config = Config()
    with run_imaged(config):
        res = unix_request(config, "DELETE", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_delete_all():
    # Example usage: move host to maintenance
    config = Config()
    for i in range(5):
        ticket = create_ticket(path="/var/run/vdsm/storage/foo%s" % i)
        server.tickets[ticket["uuid"]] = ticket
    with run_imaged(config):
        res = unix_request(config, "DELETE", "/tickets/")
    assert res.status == 204
    assert server.tickets == {}


def test_images_no_resource():
    config = Config()
    with run_imaged(config):
        res = http_request(config, "PUT", "/no/such/resource")
    assert res.status == 404


def test_images_no_method():
    config = Config()
    with run_imaged(config):
        res = http_request(config, "POST", "/images/")
    assert res.status == 405


def test_images_upload_no_request_id(tmpdir):
    payload = create_tempfile(tmpdir, "payload", "content")
    ticket = create_ticket()
    server.tickets[ticket["uuid"]] = ticket
    config = Config()
    with run_imaged(config):
        res = upload(config, ticket["uuid"], "", str(payload))
    assert res.status == 400


def test_images_upload_no_ticket_id(tmpdir):
    payload = create_tempfile(tmpdir, "payload", "content")
    request_id = str(uuid.uuid4())
    config = Config()
    with run_imaged(config):
        res = upload(config, "", request_id, str(payload))
    assert res.status == 400


def test_images_upload_no_ticket(tmpdir):
    payload = create_tempfile(tmpdir, "payload", "content")
    ticket_id = str(uuid.uuid4())
    request_id = str(uuid.uuid4())
    config = Config()
    with run_imaged(config):
        res = upload(config, ticket_id, request_id, str(payload))
    assert res.status == 403


class Config(server.Config):
    host = "127.0.0.1"
    socket = "/tmp/vdsm-imaged.sock"
    pki_dir = os.path.join(os.path.dirname(__file__), "pki")
    poll_interval = 0.1


def create_ticket(ops=("get", "put"), timeout=300, size=2**64,
                  path="/var/run/vdsm/storage/foo"):
    return {
        "uuid": str(uuid.uuid4()),
        "expires": int(time.time()) + timeout,
        "ops": list(ops),
        "size": size,
        "path": path,
    }


def upload(config, ticket_uuid, request_uuid, filename):
    uri = "/images/%s?id=%s" % (ticket_uuid, request_uuid)
    with open(filename) as f:
        return http_request(config, "PUT", uri, f)


def http_request(config, method, uri, body=None, headers=None):
    con = httplib.HTTPSConnection("127.0.0.1", config.port, config.key_file,
                                  config.cert_file)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def unix_request(config, method, uri, body=None, headers=None):
    con = uhttp.UnixHTTPSConnection(config.socket, config.key_file,
                                    config.cert_file)
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


@contextmanager
def run_imaged(config):
    server.start(config)
    try:
        yield
    finally:
        server.stop()
