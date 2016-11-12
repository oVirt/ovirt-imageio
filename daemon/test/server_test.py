# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
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
import logging
import os
import ssl
import urlparse
import uuid

import pytest

from ovirt_imageio_common import util
from ovirt_imageio_daemon import uhttp
from ovirt_imageio_daemon import server

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


class Config(server.Config):
    host = "127.0.0.1"
    socket = "/tmp/ovirt-imageio-daemon.sock"
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


def test_tickets_method_not_allowed(config):
    res = unix_request(config, "NO_SUCH_METHO", "/tickets/")
    assert res.status == httplib.METHOD_NOT_ALLOWED


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
    server_ticket = json.loads(res.read())
    # The server adds an expires key
    del server_ticket["expires"]
    assert server_ticket == ticket


def test_tickets_get_not_found(config):
    res = unix_request(config, "GET", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_put(config, monkeypatch):
    monkeypatch.setattr(util, "monotonic_time", lambda: 123456789)
    ticket = create_ticket()
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    # Server adds expires key
    ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
    server_ticket = get_ticket(ticket["uuid"])
    assert res.status == 200
    assert server_ticket == ticket


def test_tickets_put_bad_url_value(config, monkeypatch):
    monkeypatch.setattr(util, "monotonic_time", lambda: 123456789)
    ticket = create_ticket(url='http://[1.2.3.4:33')
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    assert ticket["uuid"] not in server.tickets


def test_tickets_general_exception(config, monkeypatch):
    def fail(x, y):
        raise Exception("EXPECTED FAILURE")
    monkeypatch.setattr(server.Tickets, "get", fail)
    res = unix_request(config, "GET", "/tickets/%s" % uuid.uuid4())
    error = json.loads(res.read())
    assert res.status == httplib.INTERNAL_SERVER_ERROR
    assert "application/json" in res.getheader('content-type')
    assert "EXPECTED FAILURE" in error["detail"]


def test_tickets_put_no_ticket_id(config):
    ticket = create_ticket()
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/", body)
    assert res.status == 400
    assert ticket["uuid"] not in server.tickets


def test_tickets_put_invalid_json(config):
    ticket = create_ticket()
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket,
                       "invalid json")
    assert res.status == 400
    assert ticket["uuid"] not in server.tickets


def test_tickets_put_no_timeout(config):
    ticket = create_ticket()
    del ticket["timeout"]
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    assert ticket["uuid"] not in server.tickets


def test_tickets_put_invalid_timeout(config):
    ticket = create_ticket()
    ticket["timeout"] = "invalid"
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    assert ticket["uuid"] not in server.tickets


def test_tickets_put_url_type_error(config):
    ticket = create_ticket()
    ticket["url"] = 1
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    assert ticket["uuid"] not in server.tickets


def test_tickets_put_url_scheme_not_supported(config):
    ticket = create_ticket()
    ticket["url"] = "notsupported:path"
    body = json.dumps(ticket)
    res = unix_request(config, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    assert ticket["uuid"] not in server.tickets


def test_tickets_extend(config, monkeypatch):
    now = 123456789
    monkeypatch.setattr(util, "monotonic_time", lambda: now)
    ticket = create_ticket()
    add_ticket(ticket)
    patch = {"timeout": 300}
    body = json.dumps(patch)
    now += 240
    res = unix_request(config, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    ticket["expires"] = int(now + ticket["timeout"])
    server_ticket = get_ticket(ticket["uuid"])
    assert res.status == 200
    assert server_ticket == ticket


def test_tickets_extend_no_ticket_id(config):
    ticket = create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    body = json.dumps({"timeout": 300})
    res = unix_request(config, "PATCH", "/tickets/", body)
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_invalid_json(config):
    ticket = create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    res = unix_request(config, "PATCH", "/tickets/%(uuid)s" % ticket,
                       "{invalid}")
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_no_timeout(config):
    ticket = create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    body = json.dumps({"not-a-timeout": 300})
    res = unix_request(config, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_invalid_timeout(config):
    ticket = create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    body = json.dumps({"timeout": "invalid"})
    res = unix_request(config, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_not_found(config):
    ticket_id = str(uuid.uuid4())
    body = json.dumps({"timeout": 300})
    res = unix_request(config, "PATCH", "/tickets/%s" % ticket_id, body)
    assert res.status == 404


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
        ticket = create_ticket(url="file:///var/run/vdsm/storage/foo%s" % i)
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
    res = upload(config, "", "content")
    assert res.status == 400


def test_images_upload_no_ticket(tmpdir, config):
    res = upload(config, str(uuid.uuid4()), "content")
    assert res.status == 403


def test_images_upload_forbidden(tmpdir, config):
    ticket = create_ticket(url="file:///no/such/image", ops=("read",))
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "content")
    assert res.status == 403


def test_images_upload_content_length_missing(tmpdir, config):
    ticket = create_ticket(url="file:///no/such/image")
    add_ticket(ticket)
    res = raw_http_request(config, "PUT", "/images/" + ticket["uuid"])
    assert res.status == 400


def test_images_upload_content_length_invalid(tmpdir, config):
    ticket = create_ticket(url="file:///no/such/image")
    add_ticket(ticket)
    res = raw_http_request(config, "PUT", "/images/" + ticket["uuid"],
                           headers={"content-length": "invalid"})
    assert res.status == 400


def test_images_upload_content_length_negative(tmpdir, config):
    image = create_tempfile(tmpdir, "image", "before")
    ticket = create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = raw_http_request(config, "PUT", "/images/" + ticket["uuid"],
                           headers={"content-length": "-1"})
    assert res.status == 400


def test_images_upload_no_content(tmpdir, config):
    # This is a pointless request, but valid
    image = create_tempfile(tmpdir, "image", "before")
    ticket = create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "")
    assert res.status == 200


def test_images_upload(tmpdir, config):
    image = create_tempfile(tmpdir, "image", "-------|after")
    ticket = create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "content")
    assert image.read() == "content|after"
    assert res.status == 200


@pytest.mark.parametrize("crange,before,after", [
    ("bytes 7-13/20", "before|-------|after", "before|content|after"),
    ("bytes */20", "-------|after", "content|after"),
    ("bytes */*", "-------|after", "content|after"),
])
def test_images_upload_with_range(tmpdir, config, crange, before, after):
    image = create_tempfile(tmpdir, "image", before)
    ticket = create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "content",
                 content_range=crange)
    assert image.read() == after
    assert res.status == 200


def test_images_upload_max_size(tmpdir, config):
    image_size = 100
    content = "b" * image_size
    image = create_tempfile(tmpdir, "image", "")
    ticket = create_ticket(url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], content)
    assert res.status == 200
    assert image.read() == content


def test_images_upload_too_big(tmpdir, config):
    image_size = 100
    image = create_tempfile(tmpdir, "image", "")
    ticket = create_ticket(url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "b" * (image_size + 1))
    assert res.status == 403
    assert image.read() == ""


def test_images_upload_last_byte(tmpdir, config):
    image_size = 100
    image = create_tempfile(tmpdir, "image", "a" * image_size)
    ticket = create_ticket(url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "b",
                 content_range="bytes 99-100/*")
    assert res.status == 200
    assert image.read() == "a" * 99 + "b"


def test_images_upload_after_last_byte(tmpdir, config):
    image_size = 100
    image = create_tempfile(tmpdir, "image", "a" * image_size)
    ticket = create_ticket(url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "b",
                 content_range="bytes 100-101/*")
    assert res.status == 403
    assert image.read() == "a" * image_size


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
    ticket = create_ticket()
    add_ticket(ticket)
    res = upload(config, ticket["uuid"], "content",
                 content_range=content_range)
    assert res.status == 400


@pytest.mark.parametrize("rng,start,end", [
    ("bytes=0-1023", 0, 1024),
    ("bytes=1-1023", 1, 1024),
    ("bytes=512-1023", 512, 1024),
    ("bytes=513-1023", 513, 1024),
    ("bytes=0-511", 0, 512),
    ("bytes=0-512", 0, 513),
])
def test_images_download(tmpdir, config, rng, start, end):
    data = "a" * 512 + "b" * 512
    image = create_tempfile(tmpdir, "image", data)
    ticket = create_ticket(url="file://" + str(image), size=end)
    add_ticket(ticket)
    res = download(config, ticket["uuid"], rng)
    assert res.status == 206
    received = res.read()
    assert received == data[start:end]


def test_images_download_holes(tmpdir, config):
    size = 1024
    image = create_tempfile(tmpdir, "image", size=size)
    ticket = create_ticket(url="file://" + str(image), size=size)
    add_ticket(ticket)
    res = download(config, ticket["uuid"], "bytes=0-1023")
    assert res.status == 206
    received = res.read()
    assert received == "\0" * size


@pytest.mark.parametrize("rng,end", [
    ("bytes=0-1024", 512),
])
def test_images_download_out_of_range(tmpdir, config, rng, end):
    data = "a" * 512 + "b" * 512
    image = create_tempfile(tmpdir, "image", data)
    ticket = create_ticket(url="file://" + str(image), size=end)
    add_ticket(ticket)
    res = download(config, ticket["uuid"], rng)
    assert res.status == 403
    error = json.loads(res.read())
    assert error["code"] == 403
    assert error["title"] == "Forbidden"


def create_ticket(ops=("read", "write"), timeout=300, size=2**64,
                  url="file:///var/run/vdsm/storage/foo"):
    return {
        "uuid": str(uuid.uuid4()),
        "timeout": timeout,
        "ops": list(ops),
        "size": size,
        "url": url,
    }


def upload(config, ticket_uuid, body, content_range=None):
    uri = "/images/" + ticket_uuid
    headers = {}
    if content_range is not None:
        headers["content-range"] = content_range
    return http_request(config, "PUT", uri, body=body, headers=headers)


def download(config, ticket_uuid, range):
    uri = "/images/" + ticket_uuid
    return http_request(config, "GET", uri, headers={"range": range})


def http_request(config, method, uri, body=None, headers=None):
    con = httplib.HTTPSConnection("127.0.0.1", config.port, config.key_file,
                                  config.cert_file)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def raw_http_request(config, method, uri, body=None, headers=None):
    """
    Use this to send bad requests - this will send only the headers set in
    headers, no attempt is made to create a correct request.
    """
    con = httplib.HTTPSConnection("127.0.0.1", config.port, config.key_file,
                                  config.cert_file)
    with closing(con):
        con.putrequest(method, uri)
        if headers:
            for name, value in headers.items():
                con.putheader(name, value)
        con.endheaders()
        if body:
            con.send(body)
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
    return res


# TODO: move to utils
def create_tempfile(tmpdir, name, data='', size=None):
    file = tmpdir.join(name)
    with open(str(file), 'wb') as f:
        if size is not None:
            f.seek(size - 1)
            f.write("\0")
            f.seek(0)
        if data:
            f.write(data)
    return file


def test_create_tempfile_hole(tmpdir):
    size = 1024
    file = create_tempfile(tmpdir, "image", size=size)
    assert file.read() == "\0" * size


def test_create_tempfile_data(tmpdir):
    size = 1024
    byte = "\xf0"
    data = byte * size
    file = create_tempfile(tmpdir, "image", data=data)
    assert file.read() == byte * size


def test_create_tempfile_data_and_size(tmpdir):
    virtual_size = 1024
    byte = "\xf0"
    data = byte * 512
    file = create_tempfile(tmpdir, "image", data=data, size=virtual_size)
    assert file.read() == data + "\0" * (virtual_size - len(data))


# TODO: move into tickets.py
def add_ticket(ticket):
    # Simulate adding a ticket to the server, without modifying the source
    # ticket.
    ticket = json.loads(json.dumps(ticket))
    ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
    ticket["url"] = urlparse.urlparse(ticket["url"])
    server.tickets[ticket["uuid"]] = ticket


def get_ticket(uuid):
    # Get a copy of the current server ticket, simulating a get request
    ticket = server.tickets[uuid]
    ticket = json.loads(json.dumps(ticket))
    ticket["url"] = urlparse.urlunparse(ticket["url"])
    return ticket
