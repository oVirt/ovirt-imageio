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


def test_images_download_full(daemon, proxy, tmpfile):
    # Simple download of entire image as done by stupid clients.
    data = b"x" * 128 * 1024

    with open(tmpfile, "wb") as f:
        f.write(data)

    # Add daemon ticket serving tmpfile.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=len(data))
    daemon.auth.add(ticket)

    # Add proxy ticket, proxying request to daemon.
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Download complete image.
    with http.Client(proxy.config) as c:
        res = c.request("GET", "/images/{}".format(ticket["uuid"]))
        client_data = res.read()

    assert res.status == 200
    assert client_data == data


def test_images_upload_full(daemon, proxy, tmpfile):
    # Simple upload of entire image as done by stupid clients.
    data = b"x" * 128 * 1024

    # Create empty sparse image.
    with open(tmpfile, "wb") as f:
        f.truncate(len(data))

    # Add daemon ticket serving tmpfile.
    ticket = testutil.create_ticket(
        url="file://{}".format(tmpfile),
        size=len(data))
    daemon.auth.add(ticket)

    # Add proxy ticket, proxying request to daemon.
    proxy.auth.add(proxy_ticket(daemon, ticket))

    # Upload data to image.
    with http.Client(proxy.config) as c:
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
