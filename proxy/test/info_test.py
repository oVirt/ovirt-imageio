# ovirt-imageio-proxy
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
from ovirt_imageio_proxy import version
from . import http


def test_info(proxy_server):
    res = http.request(proxy_server, "GET", "/info/")
    assert res.status == 200
    assert res.getheader("Content-Type").startswith("application/json")
    body = json.loads(res.read())
    assert body["version"] == version.string