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
from ovirt_imageio._internal import version

from .. import http


@pytest.fixture(scope="module")
def srv():
    cfg = config.load(["test/conf/daemon.conf"])
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


def test_get(srv):
    with http.RemoteClient(srv.config) as c:
        res = c.get("/info/")
        data = res.read()

    assert res.status == 200
    assert json.loads(data) == {"version": version.string}
    assert res.getheader("Access-Control-Allow-Origin") is None


def test_cors(srv):
    headers = {"Origin": "https://foo.example"}
    with http.RemoteClient(srv.config) as c:
        res = c.get("/info/", headers=headers)
        res.read()

    assert res.status == 200
    assert res.getheader("Access-Control-Allow-Origin") == "*"
