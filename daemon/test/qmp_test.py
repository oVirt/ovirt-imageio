# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import pprint

from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import qemu_img

from . import qemu
from . import qmp

log = logging.getLogger("test")


def test_query_status(tmpdir):
    # Simplest possible test.
    image = str(tmpdir.join("image.raw"))
    with open(image, "wb") as f:
        f.truncate(1024**2)

    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))

    with qemu.run(image, "raw", qmp_sock, start_cpu=False):
        with qmp.Client(qmp_sock) as c:
            r = c.execute("query-status")
            assert r["status"] == "prelaunch"


def test_add_bitmap(tmpdir):
    # Test command with arguments. This is also interesting for incremental
    # backup flows.
    image = str(tmpdir.join("image.qcow2"))
    qemu_img.create(image, "qcow2", size=1024**3)

    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))

    with qemu.run(image, "qcow2", qmp_sock, start_cpu=False):
        with qmp.Client(qmp_sock) as c:
            c.execute("block-dirty-bitmap-add", {
                "node": "file0",
                "name": "bitmap0",
            })
            node = qmp.find_node(c, image)
            log.debug("Found node:\n%s", pprint.pformat(node))

            if qemu.version() >= (6, 0, 0):
                bitmaps = node["inserted"]["dirty-bitmaps"]
            else:
                bitmaps = node["dirty-bitmaps"]
            assert bitmaps[0]["name"] == "bitmap0"
