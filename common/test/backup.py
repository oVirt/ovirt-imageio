# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import time

from contextlib import contextmanager

from ovirt_imageio_common import nbd
from ovirt_imageio_common.compat import subprocess

from . import qemu
from . import qmp

log = logging.getLogger("backup")


@contextmanager
def full_backup(tmpdir, disk, fmt, nbd_sock):
    """
    Start qemu internal nbd server using address nbd_sock, exposing disk for
    full backup, creating temporary files in tmpdir.
    """
    scratch_disk = str(tmpdir.join("scratch.qcow2"))
    create_scratch_disk(scratch_disk, disk)
    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))

    with qemu.run(disk, fmt, qmp_sock, start_cpu=False), \
            qmp.Client(qmp_sock) as c:
        start_backup(c, nbd_sock, disk, scratch_disk)
        try:
            yield
        finally:
            stop_backup(c)


def start_backup(c, nbd_sock, disk, scratch_disk):
    log.debug("Starting backup")
    start_nbd_server(c, nbd_sock)
    device = qmp.find_node(c, disk)["device"]
    add_backup_node(c, "backup-sda", scratch_disk, device)
    start_backup_job(c, "job0", "backup-sda", device)
    add_to_nbd_server(c, "backup-sda", "sda")


def stop_backup(c):
    # Give qemu time to detect that the NBD client disconnected before
    # we tear down the nbd server.
    log.debug("Waiting before tearing down nbd server")
    time.sleep(0.1)

    log.debug("Stopping backup")
    remove_from_nbd_server(c, "sda")
    stop_nbd_server(c)
    cancel_block_job(c, "job0")
    remove_backup_node(c, "backup-sda")


def create_scratch_disk(path, disk):
    log.debug("Creating scratch disk")
    subprocess.check_call(
        ["qemu-img", "create", "-f", "qcow2", "-b", disk, path])


def start_nbd_server(c, nbd_sock):
    log.debug("Starting nbd server listening on %s", nbd_sock)

    if nbd_sock.transport == "unix":
        addr = {
            "type": "unix",
            "data": {
                "path": nbd_sock.path
            }
        }
    elif nbd_sock.transport == "tcp":
        addr = {
            "type": "inet",
            "data": {
                "host": nbd_sock.host,
                # Qemu wants port as string.
                "port": str(nbd_sock.port),
            }
        }
    else:
        raise RuntimeError("Unsupported transport: {}".format(nbd_sock))

    c.execute("nbd-server-start", {"addr": addr})


def add_backup_node(c, name, scratch_disk, device):
    log.debug("Adding backup node for %s", device)

    c.execute("blockdev-add", {
        "driver": "qcow2",
        "node-name": name,
        "file": {
            "driver": "file",
            "filename": scratch_disk,
        },
        "backing": device,
    })


def start_backup_job(c, job_id, target, device):
    log.debug("Starting backup job")

    actions = [
        {
            "type": "blockdev-backup",
            "data": {
                "device": device,
                "job-id": job_id,
                "sync": "none",
                "target": target,
            }
        }
    ]

    c.execute("transaction", {
        "actions": actions
    })


def add_to_nbd_server(c, device, name):
    log.debug("Adding node to nbd server")
    c.execute("nbd-server-add", {
        "device": device,
        "name": name
    })


def remove_from_nbd_server(c, name):
    log.debug("Removing export %s from nbd server", name)
    c.execute("nbd-server-remove", {"name": name})


def stop_nbd_server(c):
    log.debug("Stopping nbd server")
    c.execute("nbd-server-stop")


def cancel_block_job(c, job_id):
    log.debug("Cancelling block job")
    c.execute("block-job-cancel", {"device": job_id})


def remove_backup_node(c, node_name):
    log.debug("Removing backup node %s", node_name)
    c.execute("blockdev-del", {"node-name": node_name})
