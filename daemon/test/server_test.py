# ovirt-imageio-daemon
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import
from __future__ import print_function

import io
import json
import logging
import os
import ssl
import time
import uuid

from contextlib import closing

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


def test_local_service_running():
    res = http.unix_request(
        config.images.socket, "OPTIONS", "/images/*")
    assert res.status == http_client.OK


def test_tickets_method_not_allowed():
    res = http.unix_request(
        config.tickets.socket, "NO_SUCH_METHO", "/tickets/")
    assert res.status == http_client.METHOD_NOT_ALLOWED


def test_tickets_no_resource():
    res = http.unix_request(config.tickets.socket, "GET", "/no/such/resource")
    assert res.status == 404


def test_tickets_no_method():
    res = http.unix_request(config.tickets.socket, "FOO", "/tickets/")
    assert res.status == 405


def test_tickets_get(fake_time):
    ticket = testutil.create_ticket(
        ops=["read"], sparse=False, dirty=False, transfer_id="123")
    auth.add(ticket)
    fake_time.now += 200
    res = http.unix_request(
        config.tickets.socket, "GET", "/tickets/%(uuid)s" % ticket)
    assert res.status == 200
    server_ticket = json.loads(res.read())
    # The server adds an expires key
    del server_ticket["expires"]
    ticket["active"] = False
    ticket["transferred"] = 0
    ticket["idle_time"] = 200
    assert server_ticket == ticket


def test_tickets_get_not_found():
    res = http.unix_request(
        config.tickets.socket, "GET", "/tickets/%s" % uuid.uuid4())
    assert res.status == 404


def test_tickets_put(fake_time):
    ticket = testutil.create_ticket(sparse=False, dirty=False)
    body = json.dumps(ticket)
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/%(uuid)s" % ticket, body)
    # Server adds expires key
    ticket["expires"] = int(util.monotonic_time()) + ticket["timeout"]
    ticket["active"] = False
    ticket["idle_time"] = 0
    server_ticket = auth.get(ticket["uuid"]).info()
    assert res.status == 200
    assert res.getheader("content-length") == "0"
    assert server_ticket == ticket


def test_tickets_put_bad_url_value(fake_time):
    ticket = testutil.create_ticket(url='http://[1.2.3.4:33')
    body = json.dumps(ticket)
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_tickets_general_exception(monkeypatch):
    def fail(*a, **kw):
        raise Exception("SECRET")
    monkeypatch.setattr(tickets.Handler, "get", fail)
    res = http.unix_request(
        config.tickets.socket, "GET", "/tickets/%s" % uuid.uuid4())
    error = res.read()
    assert res.status == http_client.INTERNAL_SERVER_ERROR
    assert b"SECRET" not in error


def test_tickets_put_no_ticket_id():
    ticket = testutil.create_ticket()
    body = json.dumps(ticket)
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/", body)
    assert res.status == 400
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_tickets_put_invalid_json():
    ticket = testutil.create_ticket()
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/%(uuid)s" % ticket,
        "invalid json")
    assert res.status == 400
    pytest.raises(KeyError, auth.get, ticket["uuid"])


# Using "timeout" confuses pytest-timeout plugin, workaround it by using
# "-timeout".
@pytest.mark.parametrize("missing", ["-timeout", "url", "size", "ops"])
def test_tickets_put_mandatory_fields(missing):
    ticket = testutil.create_ticket()
    del ticket[missing.strip("-")]
    body = json.dumps(ticket)
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_tickets_put_invalid_timeout():
    ticket = testutil.create_ticket()
    ticket["timeout"] = "invalid"
    body = json.dumps(ticket)
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_tickets_put_url_type_error():
    ticket = testutil.create_ticket()
    ticket["url"] = 1
    body = json.dumps(ticket)
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_tickets_put_url_scheme_not_supported():
    ticket = testutil.create_ticket()
    ticket["url"] = "notsupported:path"
    body = json.dumps(ticket)
    res = http.unix_request(
        config.tickets.socket, "PUT", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_tickets_extend(fake_time):
    ticket = testutil.create_ticket(sparse=False, dirty=False)
    auth.add(ticket)
    patch = {"timeout": 300}
    body = json.dumps(patch)
    fake_time.now += 240
    res = http.unix_request(
        config.tickets.socket, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    ticket["expires"] = int(fake_time.now + ticket["timeout"])
    ticket["active"] = False
    ticket["idle_time"] = 240
    server_ticket = auth.get(ticket["uuid"]).info()
    assert res.status == 200
    assert res.getheader("content-length") == "0"
    assert server_ticket == ticket


def test_tickets_extend_negative_timeout():
    ticket = testutil.create_ticket(sparse=False)
    auth.add(ticket)
    patch = {"timeout": -1}
    body = json.dumps(patch)
    res = http.unix_request(
        config.tickets.socket, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 400


def test_tickets_get_expired_ticket(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    res = http.unix_request(
        config.tickets.socket, "GET", "/tickets/%(uuid)s" % ticket)
    assert res.status == 200


def test_tickets_extend_expired_ticket(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    # Make the ticket expire.
    fake_time.now += 500
    server_ticket = auth.get(ticket["uuid"]).info()
    # Extend the expired ticket.
    body = json.dumps({"timeout": 300})
    res = http.unix_request(
        config.tickets.socket, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    assert res.status == 200
    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 800


def test_tickets_extend_no_ticket_id(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    body = json.dumps({"timeout": 300})
    res = http.unix_request(config.tickets.socket, "PATCH", "/tickets/", body)
    cur_ticket = auth.get(ticket["uuid"]).info()
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_invalid_json(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    res = http.unix_request(
        config.tickets.socket, "PATCH", "/tickets/%(uuid)s" % ticket,
        "{invalid}")
    cur_ticket = auth.get(ticket["uuid"]).info()
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_no_timeout(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    body = json.dumps({"not-a-timeout": 300})
    res = http.unix_request(
        config.tickets.socket, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    cur_ticket = auth.get(ticket["uuid"]).info()
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_invalid_timeout(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    prev_ticket = auth.get(ticket["uuid"]).info()
    body = json.dumps({"timeout": "invalid"})
    res = http.unix_request(
        config.tickets.socket, "PATCH", "/tickets/%(uuid)s" % ticket, body)
    cur_ticket = auth.get(ticket["uuid"]).info()
    assert res.status == 400
    assert cur_ticket == prev_ticket


def test_tickets_extend_not_found():
    ticket_id = str(uuid.uuid4())
    body = json.dumps({"timeout": 300})
    res = http.unix_request(
        config.tickets.socket, "PATCH", "/tickets/%s" % ticket_id, body)
    assert res.status == 404


def test_tickets_idle_time_active(fake_time, tmpdir):
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
    res = http.get("/images/" + ticket["uuid"])
    res.read(1)

    # Active ticket idle time is always 0.
    fake_time.now += 200
    assert auth.get(ticket["uuid"]).idle_time == 0


def test_tickets_idle_time_inactive(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)

    # Ticket idle time starts with ticket is added.
    assert auth.get(ticket["uuid"]).idle_time == 0

    # Simulate time passing without any request.
    fake_time.now += 200
    assert auth.get(ticket["uuid"]).idle_time == 200


def test_tickets_idle_time_put(fake_time, tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    http.put("/images/" + ticket["uuid"], "b" * 8192)
    assert auth.get(ticket["uuid"]).idle_time == 0


def test_tickets_idle_time_get(fake_time, tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    http.get("/images/" + ticket["uuid"])
    assert auth.get(ticket["uuid"]).idle_time == 0


@pytest.mark.parametrize("msg", [
    pytest.param({"op": "zero", "size": 1}, id="zero"),
    pytest.param({"op": "flush"}, id="flush"),
])
def test_tickets_idle_time_patch(fake_time, tmpdir, msg):
    image = testutil.create_tempfile(tmpdir, "image", b"a" * 8192)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    body = json.dumps(msg).encode('ascii')
    http.patch("/images/" + ticket["uuid"], body,
               headers={"content-type": "application/json"})
    assert auth.get(ticket["uuid"]).idle_time == 0


def test_tickets_idle_time_options(fake_time):
    ticket = testutil.create_ticket(url="file:///no/such/file")
    auth.add(ticket)

    # Request must reset idle time.
    fake_time.now += 200
    http.options("/images/" + ticket["uuid"])
    assert auth.get(ticket["uuid"]).idle_time == 0


def test_tickets_delete_one():
    ticket = testutil.create_ticket()
    auth.add(ticket)
    res = http.unix_request(
        config.tickets.socket, "DELETE", "/tickets/%(uuid)s" % ticket)
    assert res.status == 204
    # Note: incorrect according to RFC, but required for vdsm.
    assert res.getheader("content-length") == "0"
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_tickets_delete_one_not_found():
    res = http.unix_request(
        config.tickets.socket, "DELETE", "/tickets/no-such-ticket")
    assert res.status == 204
    # Note: incorrect according to RFC, but required for vdsm.
    assert res.getheader("content-length") == "0"


def test_tickets_delete_all():
    # Example usage: move host to maintenance
    for i in range(5):
        ticket = testutil.create_ticket(
            url="file:///var/run/vdsm/storage/foo%s" % i)
        auth.add(ticket)
    res = http.unix_request(config.tickets.socket, "DELETE", "/tickets/")
    assert res.status == 204
    # Note: incorrect according to RFC, but required for vdsm.
    assert res.getheader("content-length") == "0"
    pytest.raises(KeyError, auth.get, ticket["uuid"])


def test_images_no_resource():
    res = http.request("PUT", "/no/such/resource")
    assert res.status == 404


def test_images_no_method():
    res = http.request("FOO", "/images/")
    assert res.status == 405


def test_images_upload_no_ticket_id(tmpdir):
    res = http.put("/images/", "content")
    assert res.status == 400


def test_images_upload_no_ticket(tmpdir):
    res = http.put("/images/no-such-ticket", "content")
    assert res.status == 403


def test_images_upload_forbidden(tmpdir):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=["read"])
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "content")
    assert res.status == 403


def test_images_upload_content_length_missing(tmpdir):
    ticket = testutil.create_ticket(url="file:///no/such/image")
    auth.add(ticket)
    res = http.raw_request("PUT", "/images/" + ticket["uuid"])
    assert res.status == 400


def test_images_upload_content_length_invalid(tmpdir):
    ticket = testutil.create_ticket(url="file:///no/such/image")
    auth.add(ticket)
    res = http.raw_request("PUT", "/images/" + ticket["uuid"],
                           headers={"content-length": "invalid"})
    assert res.status == 400


def test_images_upload_content_length_negative(tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"before")
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    res = http.raw_request("PUT", "/images/" + ticket["uuid"],
                           headers={"content-length": "-1"})
    assert res.status == 400


def test_images_upload_no_content(tmpdir):
    # This is a pointless request, but valid
    image = testutil.create_tempfile(tmpdir, "image", b"before")
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "")
    assert res.status == 200


def test_images_upload_extends_ticket(tmpdir, fake_time):
    image = testutil.create_tempfile(tmpdir, "image", b"before")
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    res = http.put("/images/" + ticket["uuid"], "")
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the ticket.
    time.sleep(0.1)

    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


# TODO: test that flush actually flushes data. Current tests just verify that
# the server does not reject the query string.
@pytest.mark.parametrize("flush", [None, "y", "n"])
def test_images_upload(tmpdir, flush):
    data = b"-------|after"
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    uri = "/images/" + ticket["uuid"]
    if flush:
        uri += "?flush=" + flush
    res = http.put(uri, "content")
    with io.open(str(image), "rb") as f:
        assert f.read(len(data)) == b"content|after"
    assert res.status == 200
    assert res.getheader("content-length") == "0"


def test_images_upload_invalid_flush(tmpdir):
    ticket = testutil.create_ticket(url="file:///no/such/image")
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"] + "?flush=invalid", "data")
    assert res.status == 400


@pytest.mark.parametrize("crange,before,after", [
    ("bytes 7-13/20", b"before|-------|after", b"before|content|after"),
    ("bytes 0-6/*", b"-------|after", b"content|after"),
    ("bytes 0-*/*", b"-------|after", b"content|after"),
])
def test_images_upload_with_range(tmpdir, crange, before, after):
    image = testutil.create_tempfile(tmpdir, "image", before)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "content",
                   headers={"Content-Range": crange})
    with io.open(str(image), "rb") as f:
        assert f.read(len(after)) == after
    assert res.status == 200


def test_images_upload_max_size(tmpdir):
    image_size = 100
    content = b"b" * image_size
    image = testutil.create_tempfile(tmpdir, "image", b"")
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], content)
    assert res.status == 200
    with io.open(str(image), "rb") as f:
        assert f.read(len(content)) == content


def test_images_upload_too_big(tmpdir):
    image_size = 100
    image = testutil.create_tempfile(tmpdir, "image", b"")
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "b" * (image_size + 1))
    assert res.status == 416
    assert image.read() == ""


def test_images_upload_last_byte(tmpdir):
    image_size = 100
    image = testutil.create_tempfile(tmpdir, "image", b"a" * image_size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "b",
                   headers={"Content-Range": "bytes 99-100/*"})
    assert res.status == 200
    with io.open(str(image), "rb") as f:
        assert f.read(image_size) == b"a" * 99 + b"b"


def test_images_upload_after_last_byte(tmpdir):
    image_size = 100
    image = testutil.create_tempfile(tmpdir, "image", b"a" * image_size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "b",
                   headers={"Content-Range": "bytes 100-101/*"})
    assert res.status == 416
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
    ticket = testutil.create_ticket()
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "content",
                   headers={"Content-Range": content_range})
    assert res.status == 400


def test_images_download_no_ticket_id():
    res = http.get("/images/")
    assert res.status == http_client.BAD_REQUEST


def test_images_download_no_ticket():
    res = http.get("/images/no-such-ticket")
    assert res.status == http_client.FORBIDDEN


@pytest.mark.parametrize("rng,start,end", [
    ("bytes=0-1023", 0, 1024),
    ("bytes=1-1023", 1, 1024),
    ("bytes=512-1023", 512, 1024),
    ("bytes=513-1023", 513, 1024),
    ("bytes=0-511", 0, 512),
    ("bytes=0-512", 0, 513),
])
def test_images_download(tmpdir, rng, start, end):
    data = b"a" * 512 + b"b" * 512 + b"c" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=len(data))
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"], headers={"Range": rng})
    assert res.status == 206
    received = res.read()
    assert received == data[start:end]
    content_range = 'bytes %d-%d/%d' % (start, end-1, len(data))
    assert res.getheader("Content-Range") == content_range


def test_images_download_image_size_gt_ticket_size(tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", size=8192)
    ticket = testutil.create_ticket(url="file://" + str(image), size=4096)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"])
    assert res.status == 200
    assert len(res.read()) == 4096


def test_images_download_ticket_size_gt_image_size(tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", size=4096)
    ticket = testutil.create_ticket(url="file://" + str(image), size=8192)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"])
    assert res.status == 200
    assert len(res.read()) == 4096


def test_images_download_range_forbidden(tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", size=4096)
    ticket = testutil.create_ticket(url="file://" + str(image), size=8192)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"],
                   headers={"Range": "bytes=0-8192"})
    assert res.status == 416
    assert res.getheader("Content-Range") == "bytes */8192"


def test_images_download_range_unavailable(tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", size=8192)
    ticket = testutil.create_ticket(url="file://" + str(image), size=4096)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"],
                   headers={"Range": "bytes=0-4096"})
    assert res.status == 416
    assert res.getheader("Content-Range") == "bytes */4096"


def test_images_download_no_range(tmpdir):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"])
    assert res.status == 200
    received = res.read()
    assert received == b"\0" * size


def test_images_download_extends_ticket(tmpdir, fake_time):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    auth.add(ticket)
    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    res = http.get("/images/" + ticket["uuid"])
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the ticket.
    time.sleep(0.1)

    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


def test_images_download_empty(tmpdir):
    # Stupid edge case, but it should work, returning empty file :-)
    image = testutil.create_tempfile(tmpdir, "image")  # Empty image
    ticket = testutil.create_ticket(url="file://" + str(image), size=0)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"])
    assert res.status == 200
    data = res.read()
    assert data == b""


def test_images_download_partial_not_satistieble(tmpdir):
    # Image is smaller than ticket size - may happen if engine failed to detect
    # actual image size reported by vdsm - one byte difference is enough to
    # cause a failure.
    # See https://bugzilla.redhat.com/1512315.
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size + 1)
    auth.add(ticket)
    unsatisfiable_range = "bytes=0-%d" % size  # Max is size - 1
    res = http.get("/images/" + ticket["uuid"],
                   headers={"Range": unsatisfiable_range})
    assert res.status == http_client.REQUESTED_RANGE_NOT_SATISFIABLE


def test_images_download_partial_no_range(tmpdir):
    # The image is smaller than the tiket size, but we don't request a range,
    # so we should get the existing length of the image, since the ticket size
    # is only an upper limit. Or maybe we should treat the ticket size as the
    # expected size?
    # This is another variant of https://bugzilla.redhat.com/1512315.
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size + 1)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"])
    assert res.status == http_client.OK
    # Should return the available image data, not the ticket size. Reading this
    # response will fail with IncompleteRead.
    assert res.length == 1024


def test_images_download_partial_no_range_empty(tmpdir):
    # Image is empty, no range, should return an empty file - we return invalid
    # http response that fail on the client side with BadStatusLine: ''.
    # See https://bugzilla.redhat.com/1512312
    image = testutil.create_tempfile(tmpdir, "image")  # Empty image
    ticket = testutil.create_ticket(url="file://" + str(image), size=1024)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"])
    assert res.status == http_client.OK
    assert res.length == 0


def test_images_download_no_range_end(tmpdir):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"],
                   headers={"Range": "bytes=0-"})
    assert res.status == 206
    received = res.read()
    assert received == b"\0" * size


def test_images_download_holes(tmpdir):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"],
                   headers={"Range": "bytes=0-1023"})
    assert res.status == 206
    received = res.read()
    assert received == b"\0" * size


def test_images_download_filename_in_ticket(tmpdir):
    size = 1024
    filename = u"\u05d0.raw"  # hebrew aleph
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size,
                                    filename=filename)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"],
                   headers={"Range": "bytes=0-1023"})
    expected = "attachment; filename=\xd7\x90.raw"
    assert res.getheader("Content-Disposition") == expected


@pytest.mark.parametrize("rng,end", [
    ("bytes=0-1024", 512),
])
def test_images_download_out_of_range(tmpdir, rng, end):
    data = b"a" * 512 + b"b" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image), size=end)
    auth.add(ticket)
    res = http.get("/images/" + ticket["uuid"],
                   headers={"Range": rng})
    res.read()
    assert res.status == 416


def test_download_progress(tmpdir, monkeypatch):
    # We need to read at least one buffer to update the transfered value.
    monkeypatch.setattr(config.daemon, "buffer_size", 1024**2)

    # And we need to request enough data so the server does not complete before
    # the client read all the data.
    size = config.daemon.buffer_size * 50

    filename = tmpdir.join("image")
    with open(str(filename), 'wb') as image:
        image.truncate(size)
    ticket = testutil.create_ticket(
        url="file://" + str(filename), ops=["read"], size=size)
    auth.add(ticket)
    ticket = auth.get(ticket["uuid"])

    # No operations
    assert not ticket.active()
    assert ticket.transferred() == 0

    res = http.get("/images/" + ticket.uuid)
    res.read(config.daemon.buffer_size)

    # The server processed at least one buffer but we need to give it time to
    # touch the ticket.
    time.sleep(0.2)

    assert ticket.active()
    assert 0 < ticket.transferred() < size

    res.read()

    # The server has sent all the chunks but we need to give it time to
    # touch the ticket.
    time.sleep(0.2)

    assert not ticket.active()
    assert ticket.transferred() == size


# PATCH

def test_images_patch_unkown_op():
    body = json.dumps({"op": "unknown"}).encode("ascii")
    res = http.patch("/images/no-such-uuid", body)
    assert res.status == 400


@pytest.mark.parametrize("msg", [
    {"op": "zero", "size": 20},
    {"op": "zero", "size": 20, "offset": 10},
    {"op": "zero", "size": 20, "offset": 10, "flush": True},
    {"op": "zero", "size": 20, "offset": 10, "future": True},
])
def test_images_zero(tmpdir, msg):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    size = msg["size"]
    offset = msg.get("offset", 0)
    body = json.dumps(msg).encode("ascii")
    res = http.patch("/images/" + ticket["uuid"], body)

    assert res.status == 200
    assert res.getheader("content-length") == "0"
    with io.open(str(image), "rb") as f:
        assert f.read(offset) == data[:offset]
        assert f.read(size) == b"\0" * size
        assert f.read() == data[offset + size:]


def test_images_zero_extends_ticket(tmpdir, fake_time):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    body = json.dumps({"op": "zero", "size": 512}).encode("ascii")
    res = http.patch("/images/" + ticket["uuid"], body)
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the ticket.
    time.sleep(0.1)

    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


@pytest.mark.parametrize("msg", [
    {"op": "zero"},
    {"op": "zero", "size": "not an integer"},
    {"op": "zero", "size": -1},
    {"op": "zero", "size": 1, "offset": "not an integer"},
    {"op": "zero", "size": 1, "offset": -1},
    {"op": "zero", "size": 1, "offset": 1, "flush": "not a boolean"},
])
def test_images_zero_validation(msg):
    body = json.dumps(msg).encode("ascii")
    res = http.patch("/images/no-such-uuid", body)
    assert res.status == 400


def test_images_zero_no_ticket_id():
    body = json.dumps({"op": "zero", "size": 1}).encode("ascii")
    res = http.patch("/images/", body)
    assert res.status == 400


def test_images_zero_ticket_unknown():
    body = json.dumps({"op": "zero", "size": 1}).encode("ascii")
    res = http.patch("/images/no-such-uuid", body)
    assert res.status == 403


def test_images_zero_ticket_readonly(tmpdir):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=["read"])
    auth.add(ticket)
    body = json.dumps({"op": "zero", "size": 1}).encode("ascii")
    res = http.patch("/images/" + ticket["uuid"], body)
    assert res.status == 403


# TODO: Test that data was flushed.
@pytest.mark.parametrize("msg", [
    {"op": "flush"},
    {"op": "flush", "future": True},
])
def test_images_flush(tmpdir, msg):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    body = json.dumps(msg).encode("ascii")
    res = http.patch("/images/" + ticket["uuid"], body)

    assert res.status == 200
    assert res.getheader("content-length") == "0"


def test_images_flush_extends_ticket(tmpdir, fake_time):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = http.patch("/images/" + ticket["uuid"], body)
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the ticket.
    time.sleep(0.1)

    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


def test_images_flush_no_ticket_id():
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = http.patch("/images/", body)
    assert res.status == 400


def test_images_flush_ticket_unknown():
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = http.patch("/images/no-such-uuid", body)
    assert res.status == 403


def test_images_flush_ticket_readonly(tmpdir):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=["read"])
    auth.add(ticket)
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = http.patch("/images/" + ticket["uuid"], body)
    assert res.status == 403


# Options

def test_images_options_all():
    res = http.options("/images/*")
    allows = {"OPTIONS", "GET", "PUT", "PATCH"}
    features = {"extents", "zero", "flush"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    options = json.loads(res.read())
    assert set(options["features"]) == features
    assert options["unix_socket"] == config.images.socket


def test_images_options_read_write():
    ticket = testutil.create_ticket(ops=["read", "write"])
    auth.add(ticket)
    res = http.options("/images/" + ticket["uuid"])
    allows = {"OPTIONS", "GET", "PUT", "PATCH"}
    features = {"extents", "zero", "flush"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    assert set(json.loads(res.read())["features"]) == features


def test_images_options_read():
    ticket = testutil.create_ticket(ops=["read"])
    auth.add(ticket)
    res = http.options("/images/" + ticket["uuid"])
    allows = {"OPTIONS", "GET"}
    features = {"extents"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    assert set(json.loads(res.read())["features"]) == features


def test_images_options_write():
    ticket = testutil.create_ticket(ops=["write"])
    auth.add(ticket)
    res = http.options("/images/" + ticket["uuid"])
    # Having "write" imply also "read".
    allows = {"OPTIONS", "GET", "PUT", "PATCH"}
    features = {"extents", "zero", "flush"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    assert set(json.loads(res.read())["features"]) == features


def test_images_options_extends_ticket(fake_time):
    ticket = testutil.create_ticket()
    auth.add(ticket)
    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    res = http.options("/images/" + ticket["uuid"])
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the ticket.
    time.sleep(0.1)

    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


def test_images_options_for_no_ticket():
    res = http.options("/images/")
    assert res.status == 400


def test_images_options_for_nonexistent_ticket():
    res = http.options("/images/no-such-ticket")
    assert res.status == 403


def test_images_options_ticket_expired(fake_time):
    ticket = testutil.create_ticket(timeout=300)
    auth.add(ticket)
    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    # Make the ticket expire
    fake_time.now += 300
    res = http.options("/images/" + ticket["uuid"])
    assert res.status == 403

    server_ticket = auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300


# HTTP correctness

def test_images_response_version_success(tmpdir):
    image = testutil.create_tempfile(tmpdir, "image", b"old")
    ticket = testutil.create_ticket(url="file://" + str(image))
    auth.add(ticket)
    res = http.put("/images/" + ticket["uuid"], "new")
    assert res.status == 200
    assert res.version == 11


def test_images_response_version_error(tmpdir):
    res = http.get("/images/no-such-ticket")
    assert res.status != 200
    assert res.version == 11


@pytest.mark.parametrize("method, body", [
    ("PUT", "data"),
    ("PATCH", json.dumps({"op": "flush"}).encode("ascii")),
    ("OPTIONS", None),
    ("GET", None),
])
def test_keep_alive_connection_on_success(tmpdir, method, body):
    # After successful request the connection should remain open.
    image = testutil.create_tempfile(tmpdir, "image", size=1024)
    ticket = testutil.create_ticket(url="file://" + str(image),
                                    size=1024)
    auth.add(ticket)
    uri = "/images/%(uuid)s" % ticket
    con = http.connection()
    with closing(con):
        # Disabling auto_open so we can test if a connection was closed.
        con.auto_open = False
        con.connect()

        # Send couple of requests - all should succeed.
        for i in range(3):
            con.request(method, uri, body=body)
            r1 = http.response(con)
            r1.read()
            assert r1.status == 200


@pytest.mark.parametrize("method,body", [
    ("OPTIONS", None),
    ("GET", None),
    # Patch reads entire body before checking ticket, so connection should be
    # kept alive.
    ("PATCH", json.dumps({"op": "flush"}).encode("ascii")),
])
def test_keep_alive_connection_on_error(tmpdir, method, body):
    # When a request does not have a payload, the server can keep the
    # connection open after and error.
    uri = "/images/no-such-ticket"
    con = http.connection()
    with closing(con):
        # Disabling auto_open so we can test if a connection was closed.
        con.auto_open = False
        con.connect()

        # Send couple of requests - all should fail, without closing the
        # connection.
        for i in range(3):
            con.request(method, uri, body=body)
            r1 = http.response(con)
            r1.read()
            assert r1.status == 403


@pytest.mark.parametrize("method, body", [
    ("PUT", "data"),
])
def test_close_connection_on_errors(tmpdir, method, body):
    # When a request have a payload, the server must close the
    # connection after an error, in case the entire body was not read.
    uri = "/images/no-such-ticket"
    con = http.connection()

    # Disabling auto_open so we can test if a connection was closed.
    con.auto_open = False
    with closing(con):
        # Disabling auto_open so we can test if a connection was closed.
        con.auto_open = False
        con.connect()

        # Send the first request. It will fail before reading the
        # payload.
        con.request(method, uri, body=body)
        r1 = http.response(con)
        r1.read()
        assert r1.status == 403

        # Try to send another request. This will fail since the server closed
        # the connection, and we disabled auto_open.  Fails in request() or in
        # getresponse(), probably depends on timing.
        with pytest.raises(
                (http_client.NotConnected, http_client.BadStatusLine)):
            con.request(method, uri, body=body)
            http.response(con)
