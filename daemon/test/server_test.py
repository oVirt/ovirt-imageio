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
import sys
import uuid

from contextlib import contextmanager
from contextlib import closing
from pprint import pprint

from six.moves import http_client

import pytest

from ovirt_imageio_common import configloader
from ovirt_imageio_common import util
from ovirt_imageio_common.ssl import check_protocol
from ovirt_imageio_daemon import config
from ovirt_imageio_daemon import pki
from ovirt_imageio_daemon import uhttp
from ovirt_imageio_daemon import server
from ovirt_imageio_daemon import tickets
from test import testutils

pytestmark = pytest.mark.skipif(sys.version_info[0] > 2,
                                reason='needs porting to python 3')

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


class FakeTime(object):

    def __init__(self):
        self.now = 0

    def monotonic_time(self):
        return self.now


@pytest.fixture()
def fake_time(monkeypatch):
    time = FakeTime()
    monkeypatch.setattr(util, "monotonic_time", time.monotonic_time)
    return time


def setup_module(m):
    conf = os.path.join(os.path.dirname(__file__), "daemon.conf")
    configloader.load(config, [conf])
    server.start(config)


def teardown_module(m):
    server.stop()


def setup_function(f):
    tickets.clear()


def test_tickets_method_not_allowed():
    res = unix_request("NO_SUCH_METHO", "/tickets/")
    assert res.status == http_client.METHOD_NOT_ALLOWED


def test_tickets_no_resource():
    res = unix_request("GET", "/no/such/resource")
    assert res.status == 404


def test_tickets_no_method():
    res = unix_request("POST", "/tickets/")
    assert res.status == 405


def test_tickets_get(fake_time):
    ticket = testutils.create_ticket(ops=["read"])
    add_ticket(ticket)
    fake_time.now += 200
    res = unix_request("GET", "/tickets/%(uuid)s" % ticket)
    assert res.status == 200
    server_ticket = json.loads(res.read())
    # The server adds an expires key
    del server_ticket["expires"]
    ticket["active"] = False
    ticket["transferred"] = 0
    ticket["timeout"] = 100
    assert server_ticket == ticket


def test_tickets_get_not_found():
    res = unix_request("GET", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_put(fake_time):
    ticket = testutils.create_ticket()
    body = json.dumps(ticket)
    res = unix_request("PUT", "/tickets/%(uuid)s" % ticket, body)
    # Server adds expires key
    ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
    ticket["active"] = False
    server_ticket = get_ticket(ticket["uuid"])
    assert res.status == 200
    assert server_ticket == ticket


def test_tickets_put_bad_url_value(fake_time):
    ticket = testutils.create_ticket(url='http://[1.2.3.4:33')
    body = json.dumps(ticket)
    res = unix_request("PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_tickets_general_exception(monkeypatch):
    def fail(x, y):
        raise Exception("EXPECTED FAILURE")
    monkeypatch.setattr(server.Tickets, "get", fail)
    res = unix_request("GET", "/tickets/%s" % uuid.uuid4())
    error = json.loads(res.read())
    assert res.status == http_client.INTERNAL_SERVER_ERROR
    assert "application/json" in res.getheader('content-type')
    assert "EXPECTED FAILURE" in error["detail"]


def test_tickets_put_no_ticket_id():
    ticket = testutils.create_ticket()
    body = json.dumps(ticket)
    res = unix_request("PUT", "/tickets/", body)
    assert res.status == 400
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_tickets_put_invalid_json():
    ticket = testutils.create_ticket()
    res = unix_request("PUT", "/tickets/%(uuid)s" % ticket,
                       "invalid json")
    assert res.status == 400
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


# Using "timeout" confuses pytest-timeout plugin, workaround it by using
# "-timeout".
@pytest.mark.parametrize("missing", ["-timeout", "url", "size", "ops"])
def test_tickets_put_mandatory_fields(missing):
    ticket = testutils.create_ticket()
    del ticket[missing.strip("-")]
    body = json.dumps(ticket)
    res = unix_request("PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_tickets_put_invalid_timeout():
    ticket = testutils.create_ticket()
    ticket["timeout"] = "invalid"
    body = json.dumps(ticket)
    res = unix_request("PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_tickets_put_url_type_error():
    ticket = testutils.create_ticket()
    ticket["url"] = 1
    body = json.dumps(ticket)
    res = unix_request("PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_tickets_put_url_scheme_not_supported():
    ticket = testutils.create_ticket()
    ticket["url"] = "notsupported:path"
    body = json.dumps(ticket)
    res = unix_request("PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_tickets_extend(fake_time):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    patch = {"timeout": 300}
    body = json.dumps(patch)
    fake_time.now += 240
    res = unix_request("PATCH", "/tickets/%(uuid)s" % ticket, body)
    ticket["expires"] = int(fake_time.now + ticket["timeout"])
    ticket["active"] = False
    server_ticket = get_ticket(ticket["uuid"])
    assert res.status == 200
    assert server_ticket == ticket


def test_tickets_get_expired_ticket(fake_time):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    res = unix_request("GET", "/tickets/%(uuid)s" % ticket)
    assert res.status == 200
    server_ticket = json.loads(res.read())
    assert server_ticket["timeout"] == -200


def test_tickets_extend_expired_ticket(fake_time):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    server_ticket = get_ticket(ticket["uuid"])
    assert server_ticket["timeout"] == -200
    # Extend the expired ticket.
    body = json.dumps({"timeout": 300})
    res = unix_request("PATCH", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 200
    server_ticket = get_ticket(ticket["uuid"])
    assert server_ticket["timeout"] == 300
    fake_time.now += 100
    server_ticket = get_ticket(ticket["uuid"])
    # Timeout is still ticking.
    assert server_ticket["timeout"] == 200


def test_tickets_extend_no_ticket_id(fake_time):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    body = json.dumps({"timeout": 300})
    res = unix_request("PATCH", "/tickets/", body)
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_invalid_json(fake_time):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    res = unix_request("PATCH", "/tickets/%(uuid)s" % ticket,
                       "{invalid}")
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_no_timeout(fake_time):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    body = json.dumps({"not-a-timeout": 300})
    res = unix_request("PATCH", "/tickets/%(uuid)s" % ticket, body)
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_invalid_timeout(fake_time):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    prev_ticket = get_ticket(ticket["uuid"])
    body = json.dumps({"timeout": "invalid"})
    res = unix_request("PATCH", "/tickets/%(uuid)s" % ticket, body)
    cur_ticket = get_ticket(ticket["uuid"])
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_not_found():
    ticket_id = str(uuid.uuid4())
    body = json.dumps({"timeout": 300})
    res = unix_request("PATCH", "/tickets/%s" % ticket_id, body)
    assert res.status == 404


def test_tickets_delete_one():
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    res = unix_request("DELETE", "/tickets/%(uuid)s" % ticket)
    assert res.status == 204
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_tickets_delete_one_not_found():
    res = unix_request("DELETE", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_delete_all():
    # Example usage: move host to maintenance
    for i in range(5):
        ticket = testutils.create_ticket(
            url="file:///var/run/vdsm/storage/foo%s" % i)
        add_ticket(ticket)
    res = unix_request("DELETE", "/tickets/")
    assert res.status == 204
    pytest.raises(KeyError, tickets.get, ticket["uuid"])


def test_images_no_resource():
    res = http_request("PUT", "/no/such/resource")
    assert res.status == 404


def test_images_no_method():
    res = http_request("POST", "/images/")
    assert res.status == 405


def test_images_upload_no_ticket_id(tmpdir):
    res = upload("", "content")
    assert res.status == 400


def test_images_upload_no_ticket(tmpdir):
    res = upload(str(uuid.uuid4()), "content")
    assert res.status == 403


def test_images_upload_forbidden(tmpdir):
    ticket = testutils.create_ticket(
        url="file:///no/such/image", ops=("read",))
    add_ticket(ticket)
    res = upload(ticket["uuid"], "content")
    assert res.status == 403


def test_images_upload_content_length_missing(tmpdir):
    ticket = testutils.create_ticket(url="file:///no/such/image")
    add_ticket(ticket)
    res = raw_http_request("PUT", "/images/" + ticket["uuid"])
    assert res.status == 400


def test_images_upload_content_length_invalid(tmpdir):
    ticket = testutils.create_ticket(url="file:///no/such/image")
    add_ticket(ticket)
    res = raw_http_request("PUT", "/images/" + ticket["uuid"],
                           headers={"content-length": "invalid"})
    assert res.status == 400


def test_images_upload_content_length_negative(tmpdir):
    image = create_tempfile(tmpdir, "image", "before")
    ticket = testutils.create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = raw_http_request("PUT", "/images/" + ticket["uuid"],
                           headers={"content-length": "-1"})
    assert res.status == 400


def test_images_upload_no_content(tmpdir):
    # This is a pointless request, but valid
    image = create_tempfile(tmpdir, "image", "before")
    ticket = testutils.create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = upload(ticket["uuid"], "")
    assert res.status == 200


def test_images_upload(tmpdir):
    image = create_tempfile(tmpdir, "image", "-------|after")
    ticket = testutils.create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = upload(ticket["uuid"], "content")
    assert image.read() == "content|after"
    assert res.status == 200


@pytest.mark.parametrize("crange,before,after", [
    ("bytes 7-13/20", "before|-------|after", "before|content|after"),
    ("bytes */20", "-------|after", "content|after"),
    ("bytes */*", "-------|after", "content|after"),
])
def test_images_upload_with_range(tmpdir, crange, before, after):
    image = create_tempfile(tmpdir, "image", before)
    ticket = testutils.create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = upload(ticket["uuid"], "content",
                 content_range=crange)
    assert image.read() == after
    assert res.status == 200


def test_images_upload_max_size(tmpdir):
    image_size = 100
    content = "b" * image_size
    image = create_tempfile(tmpdir, "image", "")
    ticket = testutils.create_ticket(
        url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(ticket["uuid"], content)
    assert res.status == 200
    assert image.read() == content


def test_images_upload_too_big(tmpdir):
    image_size = 100
    image = create_tempfile(tmpdir, "image", "")
    ticket = testutils.create_ticket(
        url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(ticket["uuid"], "b" * (image_size + 1))
    assert res.status == 403
    assert image.read() == ""


def test_images_upload_last_byte(tmpdir):
    image_size = 100
    image = create_tempfile(tmpdir, "image", "a" * image_size)
    ticket = testutils.create_ticket(
        url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(ticket["uuid"], "b",
                 content_range="bytes 99-100/*")
    assert res.status == 200
    assert image.read() == "a" * 99 + "b"


def test_images_upload_after_last_byte(tmpdir):
    image_size = 100
    image = create_tempfile(tmpdir, "image", "a" * image_size)
    ticket = testutils.create_ticket(
        url="file://" + str(image), size=image_size)
    add_ticket(ticket)
    res = upload(ticket["uuid"], "b",
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
def test_images_upload_invalid_range(tmpdir, content_range):
    ticket = testutils.create_ticket()
    add_ticket(ticket)
    res = upload(ticket["uuid"], "content",
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
def test_images_download(tmpdir, rng, start, end):
    data = "a" * 512 + "b" * 512
    image = create_tempfile(tmpdir, "image", data)
    ticket = testutils.create_ticket(
        url="file://" + str(image), size=end)
    add_ticket(ticket)
    res = download(ticket["uuid"], rng)
    assert res.status == 206
    received = res.read()
    assert received == data[start:end]


def test_images_download_no_range(tmpdir):
    size = 1024
    image = create_tempfile(tmpdir, "image", size=size)
    ticket = testutils.create_ticket(url="file://" + str(image), size=size)
    add_ticket(ticket)
    res = download(ticket["uuid"])
    assert res.status == 200
    received = res.read()
    assert received == "\0" * size


def test_images_download_no_range_end(tmpdir):
    size = 1024
    image = create_tempfile(tmpdir, "image", size=size)
    ticket = testutils.create_ticket(url="file://" + str(image), size=size)
    add_ticket(ticket)
    res = download(ticket["uuid"], "bytes=0-")
    assert res.status == 206
    received = res.read()
    assert received == "\0" * size


def test_images_download_holes(tmpdir):
    size = 1024
    image = create_tempfile(tmpdir, "image", size=size)
    ticket = testutils.create_ticket(url="file://" + str(image), size=size)
    add_ticket(ticket)
    res = download(ticket["uuid"], "bytes=0-1023")
    assert res.status == 206
    received = res.read()
    assert received == "\0" * size


def test_images_download_filename_in_ticket(tmpdir):
    size = 1024
    filename = u"\u05d0.raw"  # hebrew aleph
    image = create_tempfile(tmpdir, "image", size=size)
    ticket = testutils.create_ticket(url="file://" + str(image), size=size,
                                     filename=filename)
    add_ticket(ticket)
    res = download(ticket["uuid"], "bytes=0-1023")
    expected = "attachment; filename=\xd7\x90.raw"
    assert res.getheader("Content-Disposition") == expected


@pytest.mark.parametrize("rng,end", [
    ("bytes=0-1024", 512),
])
def test_images_download_out_of_range(tmpdir, rng, end):
    data = "a" * 512 + "b" * 512
    image = create_tempfile(tmpdir, "image", data)
    ticket = testutils.create_ticket(url="file://" + str(image), size=end)
    add_ticket(ticket)
    res = download(ticket["uuid"], rng)
    assert res.status == 403
    error = json.loads(res.read())
    assert error["code"] == 403
    assert error["title"] == "Forbidden"


def test_download_progress(tmpdir):
    size = 1024**2 * 50
    filename = tmpdir.join("image")
    with open(str(filename), 'wb') as image:
        image.truncate(size)
    ticket = testutils.create_ticket(
        url="file://" + str(filename), ops=["read"], size=size)
    add_ticket(ticket)
    ticket = tickets.get(ticket["uuid"])

    # No operations
    assert not ticket.active()
    assert ticket.transferred() == 0

    res = download(ticket.uuid)

    res.read(1024**2)
    # The server has sent some chunks
    assert ticket.active()
    assert 0 < ticket.transferred() < size

    res.read()
    # The server has sent all the chunks - download completed
    assert not ticket.active()
    assert ticket.transferred() == size


@pytest.mark.parametrize("protocol", ["-ssl2", "-ssl3", "-tls1"])
def test_reject_protocols(protocol):
    rc = check_protocol("127.0.0.1", config.images.port, protocol)
    assert rc != 0


@pytest.mark.parametrize("protocol", ["-tls1_1", "-tls1_2"])
def test_accept_protocols(protocol):
    rc = check_protocol("127.0.0.1", config.images.port, protocol)
    assert rc == 0


# HTTP correctness

@pytest.mark.xfail(reason="needs investigation")
def test_images_response_version_success(tmpdir):
    image = create_tempfile(tmpdir, "image", "old")
    ticket = testutils.create_ticket(url="file://" + str(image))
    add_ticket(ticket)
    res = upload(ticket["uuid"], "new")
    assert res.status == 200
    assert res.version == 11


@pytest.mark.xfail(reason="needs investigation")
def test_images_response_version_error(tmpdir):
    res = download("no-such-ticket")
    assert res.status != 200
    assert res.version == 11


# Helpers

def upload(ticket_uuid, body, content_range=None):
    uri = "/images/" + ticket_uuid
    headers = {}
    if content_range is not None:
        headers["content-range"] = content_range
    return http_request("PUT", uri, body=body, headers=headers)


def download(ticket_uuid, range=None):
    uri = "/images/" + ticket_uuid
    headers = {"range": range} if range else None
    return http_request("GET", uri, headers=headers)


def http_request(method, uri, body=None, headers=None):
    with https_connection() as con:
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def raw_http_request(method, uri, body=None, headers=None):
    """
    Use this to send bad requests - this will send only the headers set in
    headers, no attempt is made to create a correct request.
    """
    with https_connection() as con:
        con.putrequest(method, uri)
        if headers:
            for name, value in headers.items():
                con.putheader(name, value)
        con.endheaders()
        if body:
            con.send(body)
        return response(con)


@contextmanager
def https_connection():
    con = http_client.HTTPSConnection(config.images.host,
                                      config.images.port,
                                      pki.key_file(config),
                                      pki.cert_file(config))
    with closing(con):
        yield con


def unix_request(method, uri, body=None, headers=None):
    con = uhttp.UnixHTTPConnection(config.tickets.socket)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason, res.version))
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
    ticket = tickets.Ticket(ticket)
    tickets.add(ticket.uuid, ticket)


def get_ticket(uuid):
    return tickets.get(uuid).info()
