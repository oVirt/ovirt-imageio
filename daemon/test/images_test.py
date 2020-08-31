# ovirt-imageio
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import http.client as http_client
import io
import json
import time

import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import server

from . import testutil
from . import http

# Exceptions raised when peer closed the connection and auto_open is disabled.
# Raised in request() or in getresponse(), depends on timing.
CONNECTION_CLOSED = (
    http_client.NotConnected,
    http_client.BadStatusLine,
)


BASE_FEATURES = {"checksum", "extents"}
ALL_FEATURES = BASE_FEATURES | {"zero", "flush"}


@pytest.fixture(scope="module")
def srv():
    cfg = config.load(["test/conf/daemon.conf"])
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


@pytest.fixture
def client(srv):
    with http.RemoteClient(srv.config) as c:
        yield c


def test_no_resource(srv, client):
    res = client.request("PUT", "/no/such/resource")
    assert res.status == 404


def test_no_method(srv, client):
    res = client.request("FOO", "/images/")
    assert res.status == 405


def test_upload_no_ticket_id(tmpdir, srv, client):
    res = client.put("/images/", "content")
    assert res.status == 400


def test_upload_no_ticket(tmpdir, srv, client):
    res = client.put("/images/no-such-ticket", "content")
    assert res.status == 403


def test_upload_forbidden(tmpdir, srv, client):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=["read"])
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "content")
    assert res.status == 403


def test_upload_content_length_missing(tmpdir, srv, client):
    ticket = testutil.create_ticket(url="file:///no/such/image")
    srv.auth.add(ticket)
    res = client.raw_request("PUT", "/images/" + ticket["uuid"])
    assert res.status == 400


def test_upload_content_length_invalid(tmpdir, srv, client):
    ticket = testutil.create_ticket(url="file:///no/such/image")
    srv.auth.add(ticket)
    res = client.raw_request("PUT", "/images/" + ticket["uuid"],
                             headers={"content-length": "invalid"})
    assert res.status == 400


def test_upload_content_length_negative(tmpdir, srv, client):
    image = testutil.create_tempfile(tmpdir, "image", b"before")
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    res = client.raw_request("PUT", "/images/" + ticket["uuid"],
                             headers={"content-length": "-1"})
    assert res.status == 400


def test_upload_no_content(tmpdir, srv, client):
    # This is a pointless request, but valid
    image = testutil.create_tempfile(tmpdir, "image", b"before")
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "")
    assert res.status == 200


def test_upload_extends_ticket(tmpdir, srv, client, fake_time):
    image = testutil.create_tempfile(tmpdir, "image", b"before")
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    res = client.put("/images/" + ticket["uuid"], "")
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the
    # ticket.
    time.sleep(0.1)

    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


# TODO: test that flush actually flushes data. Current tests just verify that
# the server does not reject the query string.
@pytest.mark.parametrize("flush", [None, "y", "n"])
def test_upload(tmpdir, srv, client, flush):
    data = b"-------|after"
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    uri = "/images/" + ticket["uuid"]
    if flush:
        uri += "?flush=" + flush
    res = client.put(uri, "content")
    with io.open(str(image), "rb") as f:
        assert f.read(len(data)) == b"content|after"
    assert res.status == 200
    assert res.getheader("content-length") == "0"


def test_upload_invalid_flush(tmpdir, srv, client):
    ticket = testutil.create_ticket(url="file:///no/such/image")
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"] + "?flush=invalid", "data")
    assert res.status == 400


@pytest.mark.parametrize("crange,before,after", [
    ("bytes 7-13/20", b"before|-------|after", b"before|content|after"),
    ("bytes 0-6/*", b"-------|after", b"content|after"),
    ("bytes 0-*/*", b"-------|after", b"content|after"),
])
def test_upload_with_range(tmpdir, srv, client, crange, before, after):
    image = testutil.create_tempfile(tmpdir, "image", before)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "content",
                     headers={"Content-Range": crange})
    with io.open(str(image), "rb") as f:
        assert f.read(len(after)) == after
    assert res.status == 200


def test_upload_max_size(tmpdir, srv, client):
    image_size = 100
    content = b"b" * image_size
    image = testutil.create_tempfile(tmpdir, "image", b"")
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], content)
    assert res.status == 200
    with io.open(str(image), "rb") as f:
        assert f.read(len(content)) == content


def test_upload_too_big(tmpdir, srv, client):
    image_size = 100
    image = testutil.create_tempfile(tmpdir, "image", b"")
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "b" * (image_size + 1))
    assert res.status == 416
    assert image.read() == ""


def test_upload_last_byte(tmpdir, srv, client):
    image_size = 100
    image = testutil.create_tempfile(tmpdir, "image", b"a" * image_size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "b",
                     headers={"Content-Range": "bytes 99-100/*"})
    assert res.status == 200
    with io.open(str(image), "rb") as f:
        assert f.read(image_size) == b"a" * 99 + b"b"


def test_upload_after_last_byte(tmpdir, srv, client):
    image_size = 100
    image = testutil.create_tempfile(tmpdir, "image", b"a" * image_size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "b",
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
def test_upload_invalid_range(tmpdir, srv, client, content_range):
    ticket = testutil.create_ticket()
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "content",
                     headers={"Content-Range": content_range})
    assert res.status == 400


def test_upload_close_connection(tmpdir, srv, client):
    image_size = 4096
    image = testutil.create_tempfile(tmpdir, "image", b"a" * image_size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    srv.auth.add(ticket)
    uri = "/images/" + ticket["uuid"] + "?close=y"
    data = b"b" * image_size

    # Disabling auto_open so we can test if a connection was closed.
    client.con.auto_open = False
    client.con.connect()

    res = client.put(uri, data)
    res.read()

    # Image should be updated...
    assert res.status == 200
    with io.open(str(image), "rb") as f:
        assert f.read() == data

    # But connection should be closed.
    assert res.getheader("connection") == "close"
    with pytest.raises(CONNECTION_CLOSED):
        client.put(uri, data)


def test_download_no_ticket_id(srv, client):
    res = client.get("/images/")
    assert res.status == http_client.BAD_REQUEST


def test_download_no_ticket(srv, client):
    res = client.get("/images/no-such-ticket")
    assert res.status == http_client.FORBIDDEN


@pytest.mark.parametrize("rng,start,end", [
    ("bytes=0-1023", 0, 1024),
    ("bytes=1-1023", 1, 1024),
    ("bytes=512-1023", 512, 1024),
    ("bytes=513-1023", 513, 1024),
    ("bytes=0-511", 0, 512),
    ("bytes=0-512", 0, 513),
])
def test_download(tmpdir, srv, client, rng, start, end):
    data = b"a" * 512 + b"b" * 512 + b"c" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=len(data))
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"], headers={"Range": rng})
    assert res.status == 206
    received = res.read()
    assert received == data[start:end]
    content_range = 'bytes %d-%d/%d' % (start, end-1, len(data))
    assert res.getheader("Content-Range") == content_range


def test_download_image_size_gt_ticket_size(tmpdir, srv, client):
    image = testutil.create_tempfile(tmpdir, "image", size=8192)
    ticket = testutil.create_ticket(url="file://" + str(image), size=4096)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"])
    assert res.status == 200
    assert len(res.read()) == 4096


def test_download_ticket_size_gt_image_size(tmpdir, srv, client):
    image = testutil.create_tempfile(tmpdir, "image", size=4096)
    ticket = testutil.create_ticket(url="file://" + str(image), size=8192)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"])
    assert res.status == 200
    assert len(res.read()) == 4096


def test_download_range_forbidden(tmpdir, srv, client):
    image = testutil.create_tempfile(tmpdir, "image", size=4096)
    ticket = testutil.create_ticket(url="file://" + str(image), size=8192)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"],
                     headers={"Range": "bytes=0-8192"})
    assert res.status == 416
    assert res.getheader("Content-Range") == "bytes */8192"


def test_download_range_unavailable(tmpdir, srv, client):
    image = testutil.create_tempfile(tmpdir, "image", size=8192)
    ticket = testutil.create_ticket(url="file://" + str(image), size=4096)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"],
                     headers={"Range": "bytes=0-4096"})
    assert res.status == 416
    assert res.getheader("Content-Range") == "bytes */4096"


def test_download_no_range(tmpdir, srv, client):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"])
    assert res.status == 200
    received = res.read()
    assert received == b"\0" * size


def test_download_extends_ticket(tmpdir, srv, client, fake_time):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    srv.auth.add(ticket)
    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    res = client.get("/images/" + ticket["uuid"])
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the
    # ticket.
    time.sleep(0.1)

    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


def test_download_empty(tmpdir, srv, client):
    # Stupid edge case, but it should work, returning empty file :-)
    image = testutil.create_tempfile(tmpdir, "image")  # Empty image
    ticket = testutil.create_ticket(url="file://" + str(image), size=0)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"])
    assert res.status == 200
    data = res.read()
    assert data == b""


def test_download_partial_not_satistieble(tmpdir, srv, client):
    # Image is smaller than ticket size - may happen if engine failed to detect
    # actual image size reported by vdsm - one byte difference is enough to
    # cause a failure.
    # See https://bugzilla.redhat.com/1512315.
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size + 1)
    srv.auth.add(ticket)
    unsatisfiable_range = "bytes=0-%d" % size  # Max is size - 1
    res = client.get("/images/" + ticket["uuid"],
                     headers={"Range": unsatisfiable_range})
    assert res.status == http_client.REQUESTED_RANGE_NOT_SATISFIABLE


def test_download_partial_no_range(tmpdir, srv, client):
    # The image is smaller than the tiket size, but we don't request a range,
    # so we should get the existing length of the image, since the ticket size
    # is only an upper limit. Or maybe we should treat the ticket size as the
    # expected size?
    # This is another variant of https://bugzilla.redhat.com/1512315.
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size + 1)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"])
    assert res.status == http_client.OK
    # Should return the available image data, not the ticket size. Reading
    # this response will fail with IncompleteRead.
    assert res.length == 1024


def test_download_partial_no_range_empty(tmpdir, srv, client):
    # Image is empty, no range, should return an empty file - we return invalid
    # http response that fail on the client side with BadStatusLine: ''.
    # See https://bugzilla.redhat.com/1512312
    image = testutil.create_tempfile(tmpdir, "image")  # Empty image
    ticket = testutil.create_ticket(url="file://" + str(image), size=1024)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"])
    assert res.status == http_client.OK
    assert res.length == 0


def test_download_no_range_end(tmpdir, srv, client):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"],
                     headers={"Range": "bytes=0-"})
    assert res.status == 206
    received = res.read()
    assert received == b"\0" * size


def test_download_holes(tmpdir, srv, client):
    size = 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"],
                     headers={"Range": "bytes=0-1023"})
    assert res.status == 206
    received = res.read()
    assert received == b"\0" * size


def test_download_filename_in_ticket(tmpdir, srv, client):
    size = 1024
    filename = "\u05d0.raw"  # hebrew aleph
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size,
                                    filename=filename)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"],
                     headers={"Range": "bytes=0-1023"})
    expected = "attachment; filename=\xd7\x90.raw"
    assert res.getheader("Content-Disposition") == expected


@pytest.mark.parametrize("rng,end", [
    ("bytes=0-1024", 512),
])
def test_download_out_of_range(tmpdir, srv, client, rng, end):
    data = b"a" * 512 + b"b" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image), size=end)
    srv.auth.add(ticket)
    res = client.get("/images/" + ticket["uuid"], headers={"Range": rng})
    res.read()
    assert res.status == 416


def test_download_progress(tmpdir, srv, client, monkeypatch):
    # We need to read at least one buffer to update the transfered value.
    monkeypatch.setattr(srv.config.backend_file, "buffer_size", 1024**2)

    # And we need to request enough data so the server does not complete before
    # the client read all the data.
    size = srv.config.backend_file.buffer_size * 50

    filename = tmpdir.join("image")
    with open(str(filename), 'wb') as image:
        image.truncate(size)
    ticket = testutil.create_ticket(
        url="file://" + str(filename), ops=["read"], size=size)
    srv.auth.add(ticket)
    ticket = srv.auth.get(ticket["uuid"])

    # No operations
    assert not ticket.active()
    assert ticket.transferred() == 0

    res = client.get("/images/" + ticket.uuid)
    res.read(srv.config.backend_file.buffer_size)

    # The server processed at least one buffer but we need to give it time
    # to touch the ticket.
    time.sleep(0.2)

    assert ticket.active()
    assert 0 < ticket.transferred() < size

    res.read()

    # The server has sent all the chunks but we need to give it time to
    # touch the ticket.
    time.sleep(0.2)

    assert not ticket.active()
    assert ticket.transferred() == size


def test_download_close_connection(tmpdir, srv, client):
    image_size = 4096
    image = testutil.create_tempfile(tmpdir, "image", b"a" * image_size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=image_size)
    srv.auth.add(ticket)
    uri = "/images/" + ticket["uuid"] + "?close=y"

    # Disabling auto_open so we can test if a connection was closed.
    client.con.auto_open = False
    client.con.connect()

    res = client.get(uri)
    data = res.read()

    # We should get image data...
    assert res.status == 200
    with io.open(str(image), "rb") as f:
        assert f.read() == data

    # But connection should be closed.
    assert res.getheader("connection") == "close"
    with pytest.raises(CONNECTION_CLOSED):
        client.get(uri)


# PATCH

def test_patch_unkown_op(srv, client):
    body = json.dumps({"op": "unknown"}).encode("ascii")
    res = client.patch("/images/no-such-uuid", body)
    assert res.status == 400


@pytest.mark.parametrize("msg", [
    {"op": "zero", "size": 20},
    {"op": "zero", "size": 20, "offset": 10},
    {"op": "zero", "size": 20, "offset": 10, "flush": True},
    {"op": "zero", "size": 20, "offset": 10, "future": True},
])
def test_zero(tmpdir, srv, client, msg):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    size = msg["size"]
    offset = msg.get("offset", 0)
    body = json.dumps(msg).encode("ascii")
    res = client.patch("/images/" + ticket["uuid"], body)

    assert res.status == 200
    assert res.getheader("content-length") == "0"
    with io.open(str(image), "rb") as f:
        assert f.read(offset) == data[:offset]
        assert f.read(size) == b"\0" * size
        assert f.read() == data[offset + size:]


def test_zero_extends_ticket(tmpdir, srv, client, fake_time):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    body = json.dumps({"op": "zero", "size": 512}).encode("ascii")
    res = client.patch("/images/" + ticket["uuid"], body)
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the
    # ticket.
    time.sleep(0.1)

    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


@pytest.mark.parametrize("msg", [
    {"op": "zero"},
    {"op": "zero", "size": "not an integer"},
    {"op": "zero", "size": -1},
    {"op": "zero", "size": 1, "offset": "not an integer"},
    {"op": "zero", "size": 1, "offset": -1},
    {"op": "zero", "size": 1, "offset": 1, "flush": "not a boolean"},
])
def test_zero_validation(srv, client, msg):
    body = json.dumps(msg).encode("ascii")
    res = client.patch("/images/no-such-uuid", body)
    assert res.status == 400


def test_zero_no_ticket_id(srv, client):
    body = json.dumps({"op": "zero", "size": 1}).encode("ascii")
    res = client.patch("/images/", body)
    assert res.status == 400


def test_zero_ticket_unknown(srv, client):
    body = json.dumps({"op": "zero", "size": 1}).encode("ascii")
    res = client.patch("/images/no-such-uuid", body)
    assert res.status == 403


def test_zero_ticket_readonly(tmpdir, srv, client):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=["read"])
    srv.auth.add(ticket)
    body = json.dumps({"op": "zero", "size": 1}).encode("ascii")
    res = client.patch("/images/" + ticket["uuid"], body)
    assert res.status == 403


# TODO: Test that data was flushed.
@pytest.mark.parametrize("msg", [
    {"op": "flush"},
    {"op": "flush", "future": True},
])
def test_flush(tmpdir, srv, client, msg):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    body = json.dumps(msg).encode("ascii")
    res = client.patch("/images/" + ticket["uuid"], body)

    assert res.status == 200
    assert res.getheader("content-length") == "0"


def test_flush_extends_ticket(tmpdir, srv, client, fake_time):
    data = b"x" * 512
    image = testutil.create_tempfile(tmpdir, "image", data)
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = client.patch("/images/" + ticket["uuid"], body)
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the
    # ticket.
    time.sleep(0.1)

    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


def test_flush_no_ticket_id(srv, client):
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = client.patch("/images/", body)
    assert res.status == 400


def test_flush_ticket_unknown(srv, client):
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = client.patch("/images/no-such-uuid", body)
    assert res.status == 403


def test_flush_ticket_readonly(tmpdir, srv, client):
    ticket = testutil.create_ticket(
        url="file:///no/such/image", ops=["read"])
    srv.auth.add(ticket)
    body = json.dumps({"op": "flush"}).encode("ascii")
    res = client.patch("/images/" + ticket["uuid"], body)
    assert res.status == 403


# Options

def test_options_all(srv, client):
    res = client.options("/images/*")
    allows = {"OPTIONS", "GET", "PUT", "PATCH"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    options = json.loads(res.read())
    assert set(options["features"]) == ALL_FEATURES
    assert options["unix_socket"] == srv.config.local.socket

    # Maximum connections reported only for actual ticket since this depends on
    # the backend.
    assert "max_readers" not in options
    assert "max_writers" not in options


def test_options_read_write(srv, client, tmpdir):
    size = 128 * 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=size, ops=["read", "write"])
    srv.auth.add(ticket)
    res = client.options("/images/" + ticket["uuid"])
    allows = {"OPTIONS", "GET", "PUT", "PATCH"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    options = json.loads(res.read())
    assert set(options["features"]) == ALL_FEATURES
    assert options["max_readers"] == srv.config.daemon.max_connections
    assert options["max_writers"] == 1  # Using file backend.


def test_options_read(srv, client, tmpdir):
    size = 128 * 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=size, ops=["read"])
    srv.auth.add(ticket)
    res = client.options("/images/" + ticket["uuid"])
    allows = {"OPTIONS", "GET"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    options = json.loads(res.read())
    assert set(options["features"]) == BASE_FEATURES
    assert options["max_readers"] == srv.config.daemon.max_connections
    assert options["max_writers"] == 1  # Using file backend.


def test_options_write(srv, client, tmpdir):
    size = 128 * 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=size, ops=["write"])
    srv.auth.add(ticket)
    res = client.options("/images/" + ticket["uuid"])
    # Having "write" imply also "read".
    allows = {"OPTIONS", "GET", "PUT", "PATCH"}
    assert res.status == 200
    assert set(res.getheader("allow").split(',')) == allows
    options = json.loads(res.read())
    assert set(options["features"]) == ALL_FEATURES
    assert options["max_readers"] == srv.config.daemon.max_connections
    assert options["max_writers"] == 1  # Using file backend.


def test_options_extends_ticket(srv, client, tmpdir, fake_time):
    size = 128 * 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    srv.auth.add(ticket)
    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    fake_time.now += 200
    res = client.options("/images/" + ticket["uuid"])
    assert res.status == 200

    res.read()

    # Yield to server thread - will close the opreration and extend the
    # ticket.
    time.sleep(0.1)

    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 500


def test_options_for_no_ticket(srv, client):
    res = client.options("/images/")
    assert res.status == 400


def test_options_for_nonexistent_ticket(srv, client):
    res = client.options("/images/no-such-ticket")
    assert res.status == 403


def test_options_ticket_expired(srv, client, tmpdir, fake_time):
    size = 128 * 1024
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(
        url="file://" + str(image), size=size, timeout=300)
    srv.auth.add(ticket)
    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300

    # Make the ticket expire
    fake_time.now += 300
    res = client.options("/images/" + ticket["uuid"])
    assert res.status == 403

    server_ticket = srv.auth.get(ticket["uuid"]).info()
    assert server_ticket["expires"] == 300


# HTTP correctness

def test_response_version_success(tmpdir, srv, client):
    image = testutil.create_tempfile(tmpdir, "image", b"old")
    ticket = testutil.create_ticket(url="file://" + str(image))
    srv.auth.add(ticket)
    res = client.put("/images/" + ticket["uuid"], "new")
    assert res.status == 200
    assert res.version == 11


def test_response_version_error(tmpdir, srv, client):
    res = client.get("/images/no-such-ticket")
    assert res.status != 200
    assert res.version == 11


@pytest.mark.parametrize("method, body", [
    ("PUT", "data"),
    ("PATCH", json.dumps({"op": "flush"}).encode("ascii")),
    ("OPTIONS", None),
    ("GET", None),
])
def test_keep_alive_connection_on_success(tmpdir, srv, client, method, body):
    # After successful request the connection should remain open.
    image = testutil.create_tempfile(tmpdir, "image", size=1024)
    ticket = testutil.create_ticket(url="file://" + str(image),
                                    size=1024)
    srv.auth.add(ticket)
    uri = "/images/%(uuid)s" % ticket
    # Disabling auto_open so we can test if a connection was closed.
    client.con.auto_open = False
    client.con.connect()

    # Send couple of requests - all should succeed.
    for i in range(3):
        r = client.request(method, uri, body=body)
        r.read()
        assert r.status == 200


@pytest.mark.parametrize("method, body", [
    ("OPTIONS", None),
    ("GET", None),
    # Patch reads entire body before checking ticket, so connection should be
    # kept alive.
    ("PATCH", json.dumps({"op": "flush"}).encode("ascii")),
    ("PUT", "data"),
])
def test_close_connection_on_auth_errors(tmpdir, srv, client, method, body):
    # When a request does not have a payload, the server can keep the
    # connection open after an error. However we close the connection after
    # authorization errors as part of cancellation support.
    uri = "/images/no-such-ticket"
    # Disabling auto_open so we can test if a connection was closed.
    client.con.auto_open = False
    client.con.connect()

    # Send the first request. It will fail before reading the
    # payload.
    r = client.request(method, uri, body=body)
    r.read()
    assert r.status == 403

    # Try to send another request. This will fail since the server closed
    # the connection, and we disabled auto_open.
    assert r.getheader("connection") == "close"
    with pytest.raises(CONNECTION_CLOSED):
        client.request(method, uri, body=body)


# CORS support


def test_cors_options_none(srv, client):
    res = client.options("/images/*")
    res.read()

    assert res.getheader("Access-Control-Allow-Origin") is None
    assert res.getheader("Access-Control-Allow-Headers") is None
    assert res.getheader("Access-Control-Allow-Methods") is None
    assert res.getheader("Access-Control-Max-Age") is None


def test_cors_options_some(srv, client):
    headers = {
        "Origin": "https://foo.example",
        "Access-Control-Request-Method": "PUT",
    }
    res = client.options("/images/*", headers=headers)
    res.read()

    assert res.getheader("Access-Control-Allow-Origin") == "*"
    assert res.getheader("Access-Control-Allow-Methods") == "OPTIONS,GET,PUT"
    assert res.getheader("Access-Control-Max-Age") == "86400"


def test_cors_options_all(srv, client):
    headers = {
        "Origin": "https://foo.example",
        "Access-Control-Request-Method": "PUT",
        "Access-Control-Request-Headers": "X-PINGOTHER, Content-Type",
    }
    res = client.options("/images/*", headers=headers)

    assert res.getheader("Access-Control-Allow-Origin") == "*"
    assert res.getheader("Access-Control-Allow-Headers") == "*"
    assert res.getheader("Access-Control-Allow-Methods") == "OPTIONS,GET,PUT"
    assert res.getheader("Access-Control-Max-Age") == "86400"


def test_cors_get_ok(tmpdir, srv, client):
    size = 8192
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    srv.auth.add(ticket)
    uri = "/images/" + ticket["uuid"]
    headers = {"Origin": "http://foo.example"}

    res = client.get(uri, headers=headers)
    data = res.read()

    assert res.getheader("Access-Control-Allow-Origin") == "*"
    assert res.getheader("Access-Control-Max-Age") == "86400"
    assert res.getheader("Access-Control-Allow-Headers") is None
    assert res.getheader("Access-Control-Allow-Methods") is None
    assert res.status == 200
    assert data == b"\0" * size


def test_cors_get_error(srv, client):
    headers = {"Origin": "http://foo.example"}
    res = client.get("/images/no-such-ticket", headers=headers)
    res.read()

    assert res.getheader("Access-Control-Allow-Origin") == "*"
    assert res.getheader("Access-Control-Max-Age") == "86400"
    assert res.status == 403


def test_cors_put_ok(tmpdir, srv, client):
    size = 8192
    image = testutil.create_tempfile(tmpdir, "image", size=size)
    ticket = testutil.create_ticket(url="file://" + str(image), size=size)
    srv.auth.add(ticket)
    uri = "/images/" + ticket["uuid"]
    headers = {"Origin": "http://foo.example"}
    data = b"x" * size

    res = client.put(uri, body=data, headers=headers)
    res.read()

    assert res.getheader("Access-Control-Allow-Origin") == "*"
    assert res.getheader("Access-Control-Max-Age") == "86400"
    assert res.status == 200

    with open(image, "rb") as f:
        assert f.read() == data


def test_cors_put_error(srv, client):
    headers = {"Origin": "http://foo.example"}
    res = client.put("/images/no-such-ticket", body=b"x", headers=headers)
    res.read()

    assert res.getheader("Access-Control-Allow-Origin") == "*"
    assert res.getheader("Access-Control-Max-Age") == "86400"
    assert res.status == 403
