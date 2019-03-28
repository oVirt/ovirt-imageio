# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import subprocess
import time

from contextlib import contextmanager

from . import qemu
from . import qmp

log = logging.getLogger("backup")


@contextmanager
def full_backup(disk, fmt, tmpdir):
    """
    Start nbd server, exposing disk for full backup, creating temporary files
    in tmpdir.
    """
    log.debug("Creating scratch disk")
    scratch_disk = str(tmpdir.join("scratch.qcow2"))
    subprocess.check_call([
        "qemu-img",
        "create",
        "-f", "qcow2",
        "-b", disk,
        scratch_disk
    ])

    qmp_sock = str(tmpdir.join("qmp.sock"))
    nbd_sock = str(tmpdir.join("nbd.sock"))
    backup_url = "nbd:unix:{}:exportname=sda".format(nbd_sock)

    with qemu.run(disk, fmt, qmp_sock, start_cpu=False), \
            qmp.Client(qmp_sock) as c:
        log.debug("Starting nbd server")
        c.execute("nbd-server-start", {
            "addr": {
                "type": "unix",
                "data": {
                    "path": nbd_sock,
                }
            }
        })

        node = qmp.find_node(c, disk)
        log.debug("Adding backup node for %s", node)
        c.execute("blockdev-add", {
            "driver": "qcow2",
            "node-name": "backup-sda",
            "file": {
                "driver": "file",
                "filename": scratch_disk,
            },
            "backing": node["device"],
        })

        log.debug("Starting backup job")
        c.execute("transaction", {
            'actions': [
                {
                    'data': {
                        'device': node["device"],
                        'job-id': 'backup-sda',
                        'sync': 'none',
                        'target': 'backup-sda'
                    },
                    'type': 'blockdev-backup',
                },
            ]
        })

        log.debug("Adding node to nbd server")
        c.execute("nbd-server-add", {
            "device": "backup-sda",
            "name": "sda"
        })

        try:
            yield backup_url
        finally:
            # Give qemu time to detect that the NBD client disconnected before
            # we tear down the nbd server.
            log.debug("Waiting before tearing down nbd server")
            time.sleep(0.1)

            log.debug("Removing disk sda from nbd server")
            c.execute("nbd-server-remove", {"name": "sda"})

            log.debug("Stopping nbd server")
            c.execute("nbd-server-stop")

            log.debug("Cancelling block job")
            c.execute("block-job-cancel", {"device": "backup-sda"})

            log.debug("Removing backup node")
            c.execute("blockdev-del", {"node-name": "backup-sda"})
