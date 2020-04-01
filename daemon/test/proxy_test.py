# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

from ovirt_imageio import config
from ovirt_imageio import server

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


@pytest.mark.parametrize("align", [-4096, 0, 4096])
def test_images_download_full(daemon, proxy, tmpfile, align):
    # Simple download of entire image as done by stupid clients.
    size = proxy.config.daemon.buffer_size + align
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


def test_images_download_error(daemon, proxy, tmpfile):
    # Passing error from daemon ot the proxy client.

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


@pytest.mark.parametrize("align", [-4096, 0, 4096])
def test_images_upload_full(daemon, proxy, tmpfile, align):
    # Simple upload of entire image as done by stupid clients.
    size = proxy.config.daemon.buffer_size + align
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


def proxy_ticket(daemon, daemon_ticket):
    """
    Create a proxy ticket from daemon ticket.
    """
    ticket = dict(daemon_ticket)

    host, port = daemon.remote_service.address
    ticket["url"] = "https://{}:{}/images/{}".format(
        host, port, ticket["uuid"])

    return ticket
