# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import subprocess

from . import qmp
from . import qemu

import pytest

log = logging.getLogger("test")


@pytest.mark.xfail(reason="broken on Fedora with qemu-4.0.0-rc0")
def test_query_status(tmpdir):
    # Simplest possible test.
    image = str(tmpdir.join("image.raw"))
    with open(image, "wb") as f:
        f.truncate(1024**2)

    qmp_sock = str(tmpdir.join("qmp.sock"))

    with qemu.run(image, "raw", qmp_sock, start_cpu=False):
        with qmp.Client(qmp_sock) as c:
            r = c.execute("query-status")
            assert r["status"] == "prelaunch"


@pytest.mark.xfail(reason="broken on Fedora with qemu-4.0.0-rc0")
def test_add_bitmap(tmpdir):
    # Test command with arguments. This is also interesting for incremental
    # backup flows.
    image = str(tmpdir.join("image.qcow2"))
    subprocess.check_call(["qemu-img", "create", "-f", "qcow2", image, "1g"])

    qmp_sock = str(tmpdir.join("qmp.sock"))

    with qemu.run(image, "qcow2", qmp_sock, start_cpu=False):
        with qmp.Client(qmp_sock) as c:
            b = qmp.find_node(c, image)
            c.execute("block-dirty-bitmap-add", {
                "node": b["device"],
                "name": "bitmap0",
            })
            b = qmp.find_node(c, image)
            assert b["dirty-bitmaps"][0]["name"] == "bitmap0"
