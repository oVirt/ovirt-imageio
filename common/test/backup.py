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

from six.moves.urllib.parse import urlparse

from ovirt_imageio_common import nbd
from ovirt_imageio_common import nbdutil

from . import qemu
from . import qemu_img
from . import qemu_nbd
from . import qmp

log = logging.getLogger("backup")


@contextmanager
def full_backup(tmpdir, disk, fmt, nbd_sock, checkpoint=None):
    """
    Start qemu internal nbd server using address nbd_sock, exposing disk for
    full backup, creating temporary files in tmpdir.
    """
    scratch_disk = str(tmpdir.join("scratch.qcow2"))
    create_scratch_disk(scratch_disk, disk)
    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))

    with qemu.run(disk, fmt, qmp_sock, start_cpu=False, shutdown_timeout=10), \
            qmp.Client(qmp_sock) as c, \
            run(c, nbd_sock, scratch_disk, checkpoint=checkpoint):
        yield


@contextmanager
def run(c, nbd_sock, scratch_disk, checkpoint=None):
    start_backup(c, nbd_sock, scratch_disk, checkpoint=checkpoint)
    try:
        yield
    finally:
        stop_backup(c)


def start_backup(c, nbd_sock, scratch_disk, checkpoint=None):
    log.info("Statring backup checkpoint=%s", checkpoint)
    start_nbd_server(c, nbd_sock)
    # Use node name "file0" as a stable reference to our disk. It will not
    # change when the block graph is modifed during backup.
    add_backup_node(c, "backup-sda", scratch_disk, "file0")
    start_backup_job(c, "job0", "backup-sda", "file0", checkpoint=checkpoint)
    add_to_nbd_server(c, "backup-sda", "sda")


def stop_backup(c):
    log.info("Stopping backup")
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
    qemu_img.create(path, "qcow2", backing=disk)


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


def add_backup_node(c, name, scratch_disk, node_name):
    log.debug("Adding backup node for %s", node_name)

    c.execute("blockdev-add", {
        "driver": "qcow2",
        "node-name": name,
        "file": {
            "driver": "file",
            "filename": scratch_disk,
        },
        "backing": node_name,
    })


def start_backup_job(c, job_id, target, node_name, checkpoint=None):
    log.debug("Starting backup job")
    actions = []

    if checkpoint:
        actions.append({
            "type": "block-dirty-bitmap-add",
            "data": {
                "name": checkpoint,
                "node": node_name,
                "persistent": True,
            }
        })

    actions.append({
        "type": "blockdev-backup",
        "data": {
            "device": node_name,
            "job-id": job_id,
            "sync": "none",
            "target": target,
        }
    })

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


# Backup data helpers.


def copy_disk(nbd_url, backup_disk):
    log.info("Backing up %s to %s", nbd_url, backup_disk)
    backup_url = urlparse(nbd_url)

    with nbd.open(backup_url) as src_client, \
            qemu_nbd.open(backup_disk, "qcow2") as dst_client:
        nbdutil.copy(src_client, dst_client)
