# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import server

from . import http
from . import testutil


@pytest.fixture(scope="module")
def daemon():
    daemon = server.Server(config.load(["test/conf/daemon.conf"]))
    daemon.start()
    yield daemon
    daemon.stop()


@pytest.fixture(scope="module")
def proxy():
    proxy = server.Server(config.load(["test/conf/proxy.conf"]))
    proxy.start()
    yield proxy
    proxy.stop()


def test_local_service_disabled(proxy):
    assert proxy.local_service is None


def test_images_extents(daemon, proxy, tmpfile):
    # Get image extents via proxy.
    size = 128 * 1024

    with open(tmpfile, "wb") as f:
        f.truncate(size)

    # Add tickets.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size)
    daemon.auth.add(ticket)
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Get zero extents.
    with http.RemoteClient(proxy.config) as c:
        res = c.request("GET", "/images/{}/extents".format(ticket["uuid"]))
        data = res.read()

    assert res.status == 200

    extents = json.loads(data)
    assert extents == [
        {"start": 0, "length": size, "zero": False, "hole": False}
    ]


@pytest.mark.parametrize("align", [-4096, 0, 4096])
def test_images_download_full(daemon, proxy, tmpfile, align):
    # Simple download of entire image as done by stupid clients.
    size = proxy.config.backend_file.buffer_size + align
    data = b"x" * size

    with open(tmpfile, "wb") as f:
        f.write(data)

    # Add daemon ticket serving tmpfile.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size)
    daemon.auth.add(ticket)

    # Add proxy ticket, proxying request to daemon.
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Download complete image.
    with http.RemoteClient(proxy.config) as c:
        res = c.request("GET", "/images/{}".format(ticket["uuid"]))
        client_data = res.read()

    assert res.status == 200
    assert client_data == data


def test_images_download_options_error(daemon, proxy, tmpfile):
    # Passing OPTIONS error from daemon to proxy client.

    # Create a proxy ticket, but no daemon ticket.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=4096)
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # This request should fail in the proxy when opening the backend and
    # sending OPTIONS request.
    with http.RemoteClient(proxy.config) as c:
        res = c.request("GET", "/images/{}".format(ticket["uuid"]))
        res.read()

    # The error should propagate to the caller.
    assert res.status == 403


def test_images_download_get_error(daemon, proxy, tmpfile):
    # Passing GET error from daemon to proxy client.

    size = 128 * 1024
    data = b"x" * size

    with open(tmpfile, "wb") as f:
        f.write(data)

    # Add a daemon ticket with smaller size to trigger 416 error.
    dt = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size - 4096)
    daemon.auth.add(dt)

    # Add proxy ticket with correct size to ensure the request will fail in the
    # daemon.
    pt = proxy_ticket(daemon, dt)
    pt["size"] = size
    proxy.auth.add(pt)

    # This request should fail in the daemon when trying to read after the
    # ticket size.
    with http.RemoteClient(proxy.config) as c:
        res = c.request("GET", "/images/{}".format(dt["uuid"]))
        res.read()

    # The error should propagate to the caller.
    assert res.status == 416


def test_images_download_missing_image(daemon, proxy):
    # Passing GET /extents error from daemon to proxy client.

    # Add a daemon ticket with non-existing file, causing a failure when
    # opening the backend in the daemon.
    ticket = testutil.create_ticket(
        url="file:///no/such/image",
        size=128 * 1024)

    daemon.auth.add(ticket)
    proxy.auth.add(proxy_ticket(daemon, ticket))

    with http.RemoteClient(proxy.config) as c:
        res = c.request("GET", "/images/{}".format(ticket["uuid"]))
        res.read()

    # Pass the daemon error to proxy client.
    assert res.status == 500


@pytest.mark.parametrize("align", [-4096, 0, 4096])
def test_images_upload_full(daemon, proxy, tmpfile, align):
    # Simple upload of entire image as done by stupid clients.
    size = proxy.config.backend_file.buffer_size + align
    data = b"x" * size

    # Create empty sparse image.
    with open(tmpfile, "wb") as f:
        f.truncate(size)

    # Add daemon ticket serving tmpfile.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size)
    daemon.auth.add(ticket)

    # Add proxy ticket, proxying request to daemon.
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Upload data to image.
    with http.RemoteClient(proxy.config) as c:
        res = c.request(
            "PUT",
            "/images/{}".format(ticket["uuid"]),
            body=data)
        res.read()

    assert res.status == 200

    with open(tmpfile, "rb") as f:
        assert f.read() == data


def test_images_upload_error(daemon, proxy, tmpfile):
    # Pass PUT error from daemon to proxy client.

    size = 128 * 1024
    data = b"x" * size

    # Create empty sparse image.
    with open(tmpfile, "wb") as f:
        f.truncate(size)

    # Add a daemon ticket with smaller size to trigger 416 error.
    dt = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size - 4096)
    daemon.auth.add(dt)

    # Add proxy ticket with correct size to ensure the request will fail in the
    # daemon.
    pt = proxy_ticket(daemon, dt)
    pt["size"] = size
    proxy.auth.add(pt)

    # Upload data to image.
    with http.RemoteClient(proxy.config) as c:
        res = c.request(
            "PUT",
            "/images/{}".format(dt["uuid"]),
            body=data)
        res.read()

    assert res.status == 416


def test_images_zero(daemon, proxy, tmpfile):
    # Zero entire image via proxy.
    size = 128 * 1024

    # Create image with data.
    with open(tmpfile, "wb") as f:
        f.write(b"x" * size)

    # Add tickets.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size)
    daemon.auth.add(ticket)
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Zero entire image.
    patch = {"op": "zero", "offset": 0, "size": size}
    body = json.dumps(patch).encode("utf-8")

    with http.RemoteClient(proxy.config) as c:
        res = c.request(
            "PATCH",
            "/images/{}".format(ticket["uuid"]),
            body=body)
        res.read()

    assert res.status == 200

    with open(tmpfile, "rb") as f:
        assert f.read() == b"\0" * size


def test_images_zero_error(daemon, proxy):
    # Pass PATCH error from daemon to proxy client.
    size = 128 * 1024

    # Add tickets for non existing image.
    ticket = testutil.create_ticket(
        url="file:///no/such/image",
        size=size)
    daemon.auth.add(ticket)
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Zero entire image.
    patch = {"op": "zero", "offset": 0, "size": size}
    body = json.dumps(patch).encode("utf-8")

    with http.RemoteClient(proxy.config) as c:
        res = c.request(
            "PATCH",
            "/images/{}".format(ticket["uuid"]),
            body=body)
        res.read()

    assert res.status == 500


def test_images_flush(daemon, proxy, tmpfile):
    # Zero entire image via proxy.
    size = 128 * 1024

    # Create empty sparse image.
    with open(tmpfile, "wb") as f:
        f.truncate(size)

    # Add tickets.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size)
    daemon.auth.add(ticket)
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Flush image.
    patch = {"op": "flush"}
    body = json.dumps(patch).encode("utf-8")

    with http.RemoteClient(proxy.config) as c:
        res = c.request(
            "PATCH",
            "/images/{}".format(ticket["uuid"]),
            body=body)
        res.read()

    assert res.status == 200


@pytest.mark.parametrize("conf_files", [
    # Using system PKI for both proxy and daemon (default oVirt setup).
    pytest.param(["test/conf/proxy.conf"], id="system"),
    # Using user PKI for proxy and system PKI for daemon (oVirt setup with user
    # certificates).
    pytest.param(
        ["test/conf/proxy.conf", "test/conf/user-tls.conf"], id="user")
])
def test_tls(daemon, tmpfile, conf_files):
    size = daemon.config.backend_file.buffer_size
    data = b"x" * size

    with open(tmpfile, "wb") as f:
        f.write(data)

    # Add daemon ticket serving tmpfile.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=size)
    daemon.auth.add(ticket)

    proxy = server.Server(config.load(conf_files))
    proxy.start()
    try:
        # Add proxy ticket, proxying request to daemon.
        proxy.auth.add(proxy_ticket(daemon, ticket))

        # Download complete image.
        with http.RemoteClient(proxy.config) as c:
            res = c.request("GET", "/images/{}".format(ticket["uuid"]))
            client_data = res.read()
    finally:
        proxy.stop()

    assert res.status == 200
    assert client_data == data


def proxy_ticket(daemon, daemon_ticket):
    """
    Create a proxy ticket from daemon ticket.
    """
    ticket = dict(daemon_ticket)

    host, port = daemon.remote_service.address
    ticket["url"] = "https://{}:{}/images/{}".format(
        host, port, ticket["uuid"])

    return ticket
