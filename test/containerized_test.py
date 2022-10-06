# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import json
import http.client as http_client

from ovirt_imageio._internal import config
from ovirt_imageio._internal import server
from ovirt_imageio._internal.units import KiB

from . import testutil
from . import http


TICKET_SIZE = 16 * KiB


def test_start_with_ticket(tmpdir):
    # Create ticket
    image = testutil.create_tempfile(tmpdir, name="disk.raw", size=TICKET_SIZE)
    ticket = testutil.create_ticket(size=TICKET_SIZE, url=f'file://{image}')
    ticket_id = ticket['uuid']
    ticket_path = tmpdir.join("file.json")
    ticket_path.write(json.dumps(ticket))

    # Start server with ticket
    cfg = config.load("test/conf/daemon.conf")
    srv = server.Server(cfg, ticket=ticket_path)
    srv.start()
    try:
        data = b"a" * (TICKET_SIZE // 2) + b"b" * (TICKET_SIZE // 2)
        # Test that we can upload
        with http.RemoteClient(srv.config) as c:
            res = c.put(f'/images/{ticket_id}', data)
            assert res.status == http_client.OK
        # Test that we can download and matches the uploaded data
        with http.RemoteClient(srv.config) as c:
            res = c.get(f'/images/{ticket_id}')
            assert res.read() == data
            assert res.status == http_client.OK
    finally:
        srv.stop()
