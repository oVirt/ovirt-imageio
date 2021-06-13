# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import time

from contextlib import contextmanager

from urllib.parse import urlparse

from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import nbdutil
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd

from . import qemu
from . import qmp

log = logging.getLogger("backup")


@contextmanager
def full_backup(tmpdir, disk, fmt, sock, checkpoint=None):
    """
    Start qemu internal nbd server using address sock, exposing disk for
    full backup, creating temporary files in tmpdir.
    """
    scratch_disk = str(tmpdir.join("scratch.qcow2"))
    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))

    disk_size = qemu_img.info(disk)["virtual-size"]
    qemu_img.create(scratch_disk, "qcow2", size=disk_size)

    with qemu.run(disk, fmt, qmp_sock, start_cpu=False, shutdown_timeout=10), \
            qmp.Client(qmp_sock) as c, \
            run(c, sock, scratch_disk, checkpoint=checkpoint):
        yield


@contextmanager
def run(qmp, sock, scratch_disk, checkpoint=None, incremental=None):
    b = Backup(qmp, sock, scratch_disk, checkpoint=checkpoint,
               incremental=incremental)
    b.start()
    try:
        yield b
    finally:
        b.stop()


class Backup:

    def __init__(self, qmp, sock, scratch_disk, checkpoint=None,
                 incremental=None):
        self.qmp = qmp
        self.sock = sock
        self.scratch_disk = scratch_disk
        self.checkpoint = checkpoint
        self.incremental = incremental

        # Hardcoded value, good enough for now.
        self.export = "sda"

        # Use node name "file0" as a stable reference to our disk. It will not
        # change when the block graph is modifed during backup.
        self.file = "file0"

        self.job = "job0"

        # Libvirt uses something like "backup-libvirt-42-format".
        self.node = "backup-file0"
        self.bitmap = self.node if self.incremental else None

    def start(self):
        log.info("Starting backup checkpoint=%s incremental=%s",
                 self.checkpoint, self.incremental)

        self.start_nbd_server()
        self.add_backup_node()

        if self.incremental:
            self.add_dirty_bitmap()

        self.run_backup_transaction()
        self.add_to_nbd_server()

    def stop(self):
        log.info("Stopping backup checkpoint=%s incremental=%s",
                 self.checkpoint, self.incremental)

        # Give qemu time to detect that the NBD client disconnected before
        # we tear down the nbd server.
        log.debug("Waiting before tearing down nbd server")
        time.sleep(0.1)

        self.remove_from_nbd_server()
        self.stop_nbd_server()
        self.cancel_block_job()
        self.remove_backup_node()

        if self.incremental:
            self.remove_dirty_bitmap()

    def start_nbd_server(self):
        log.debug("Starting nbd server listening on %s", self.sock)

        if self.sock.transport == "unix":
            addr = {"type": "unix",
                    "data": {"path": self.sock.path}}
        elif self.sock.transport == "tcp":
            addr = {"type": "inet",
                    "data": {"host": self.sock.host,
                             "port": str(self.sock.port)}}
        else:
            raise RuntimeError("Unsupported transport: {}".format(self.sock))

        self.qmp.execute("nbd-server-start", {"addr": addr})

    def add_backup_node(self):
        log.debug("Adding backup node %s for %s", self.node, self.file)

        self.qmp.execute("blockdev-add", {
            "driver": "qcow2",
            "node-name": self.node,
            "file": {
                "driver": "file",
                "filename": self.scratch_disk,
            },
            "backing": self.file,
        })

    def add_dirty_bitmap(self):
        """
        Real backup code get a list of all checkpoints since incremental, and
        merge all of them into the temporary bitmap.
        """
        log.debug("Adding dirty bitmap %s for incremental backup since: %s",
                  self.bitmap, self.incremental)

        self.qmp.execute("block-dirty-bitmap-add", {
            "node": self.file,
            "name": self.bitmap,
            "disabled": True,
        })

        self.qmp.execute("block-dirty-bitmap-merge", {
            "node": self.file,
            "target": self.bitmap,
            "bitmaps": [self.incremental],
        })

    def run_backup_transaction(self):
        log.debug("Running backup transaction")
        actions = []

        if self.incremental:
            # Disbale the previous active dirty bitmap. Changes after this
            # point will be recorded in the new bitmap.
            actions.append({
                "type": "block-dirty-bitmap-disable",
                "data": {
                    "node": self.file,
                    "name": self.incremental,
                }
            })

        if self.checkpoint:
            actions.append({
                "type": "block-dirty-bitmap-add",
                "data": {
                    "name": self.checkpoint,
                    "node": self.file,
                    "persistent": True,
                }
            })

        actions.append({
            "type": "blockdev-backup",
            "data": {
                "device": self.file,
                "job-id": self.job,
                "sync": "none",
                "target": self.node,
            }
        })

        self.qmp.execute("transaction", {"actions": actions})

    def add_to_nbd_server(self):
        log.debug("Adding node %s and bitmap %s to nbd server",
                  self.node, self.bitmap)
        arguments = {
            "type": "nbd",
            "id": self.export,
            "node-name": self.node,
            "name": self.export,
            "allocation-depth": True,
        }
        if self.bitmap:
            arguments["bitmaps"] = [self.bitmap]
        self.qmp.execute("block-export-add", arguments)

    def remove_from_nbd_server(self):
        log.debug("Removing export %s from nbd server", self.export)
        self.qmp.execute("block-export-del", {
            "id": self.export,
            "mode": "hard",
        })

    def stop_nbd_server(self):
        log.debug("Stopping nbd server")
        self.qmp.execute("nbd-server-stop")

    def cancel_block_job(self):
        log.debug("Cancelling block job %s", self.job)
        self.qmp.execute("block-job-cancel", {"device": self.job})

    def remove_backup_node(self):
        log.debug("Removing backup node %s", self.node)
        self.qmp.execute("blockdev-del", {"node-name": self.node})

    def remove_dirty_bitmap(self):
        log.debug("Removing dirty bitmap %s from node %s",
                  self.bitmap, self.file)
        self.qmp.execute("block-dirty-bitmap-remove", {
            "node": self.file,
            "name": self.bitmap,
        })


# Backup data helpers.


def copy_disk(nbd_url, backup_disk):
    log.info("Backing up data extents from %s to %s", nbd_url, backup_disk)
    backup_url = urlparse(nbd_url)

    with nbd.open(backup_url) as src_client, \
            qemu_nbd.open(backup_disk, "qcow2") as dst_client:
        nbdutil.copy(src_client, dst_client)


def copy_dirty(nbd_url, backup_disk):
    log.info("Backing up dirty extents from %s to %s", nbd_url, backup_disk)
    backup_url = urlparse(nbd_url)

    with nbd.open(backup_url, dirty=True) as src_client, \
            qemu_nbd.open(backup_disk, "qcow2") as dst_client:

        buf = bytearray(4 * 1024**2)

        offset = 0
        for ext in nbdutil.extents(src_client, dirty=True):
            if ext.dirty:
                todo = ext.length
                while todo:
                    step = min(todo, len(buf))
                    view = memoryview(buf)[:step]
                    src_client.readinto(offset, view)
                    dst_client.write(offset, view)
                    offset += step
                    todo -= step
            else:
                offset += ext.length
