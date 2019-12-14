# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import logging
import os

import pytest

from ovirt_imageio_common import nbd

from . import backup
from . import qemu
from . import qemu_img
from . import qemu_nbd
from . import qmp
from . import testutil

log = logging.getLogger("test")


@pytest.mark.parametrize("transport", ["unix", "tcp"])
@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_full_backup(tmpdir, fmt, transport):
    disk_size = 1024**2
    disk_part = disk_size // 4
    disk = str(tmpdir.join("disk." + fmt))
    backup_disk = str(tmpdir.join("backup.raw"))

    # Create disk
    qemu_img.create(disk, fmt, size=disk_size)

    # Pupulate disk with data.
    with qemu_nbd.open(disk, fmt) as d:
        for i in range(0, disk_size, disk_part):
            data = b"%d\n" % i
            d.write(i, data.ljust(512))
        d.flush()

    if transport == "unix":
        nbd_sock = nbd.UnixAddress(tmpdir.join("nbd.sock"))
    else:
        nbd_sock = nbd.TCPAddress("localhost", testutil.random_tcp_port())

    # Backup using qemu-img convert.
    with backup.full_backup(tmpdir, disk, fmt, nbd_sock):
        log.debug("Backing up image with qemu-img")
        qemu_img.convert(
            nbd_sock.url("sda"),
            backup_disk,
            src_fmt="raw",
            dst_fmt="raw",
            progress=True)

    # Compare source and backup disks.
    with qemu_nbd.open(disk, fmt, read_only=True) as d, \
            io.open(backup_disk, "rb") as b:
        for i in range(0, disk_size, disk_part):
            b.seek(i)
            assert d.read(i, 512) == b.read(512)


# This can take more than 30 seoconds when running on Travis without hardware
# acceleration:
# 41.77s setup    test/backup_test.py::test_full_backup_guest
# 17.59s call     test/backup_test.py::test_full_backup_guest
@pytest.mark.timeout(120)
@pytest.mark.xfail(
    "OVIRT_CI" in os.environ, reason="Always times out", run=False)
def test_full_backup_guest(tmpdir, base_image):
    disk_size = qemu_img.info(base_image)["virtual-size"]

    disk = str(tmpdir.join("disk.qcow2"))
    qemu_img.create(disk, "qcow2", backing=base_image)

    scratch_disk = str(tmpdir.join("scratch.qcow2"))
    qemu_img.create(scratch_disk, "qcow2", size=disk_size)

    backup_disk = str(tmpdir.join("backup.qcow2"))
    qemu_img.create(backup_disk, "qcow2", size=disk_size)

    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))
    nbd_sock = nbd.UnixAddress(tmpdir.join("nbd.sock"))

    log.info("Starting vm")

    with qemu.run(disk, "qcow2", qmp_sock) as guest, \
            qmp.Client(qmp_sock) as qmp_client:

        log.info("Logging in")
        guest.login("root", "")

        log.info("Creating file before backup")
        assert guest.run("touch before-backup; sync") == ""

        log.info("Statring full backup check1")
        backup.start_backup(
            qmp_client, nbd_sock, scratch_disk, checkpoint="check1")

        log.info("Creating file during backup")
        assert guest.run("touch during-backup; sync") == ""

        log.info("Backing up disk to %s", backup_disk)
        qemu_img.convert(
            nbd_sock.url("sda"), backup_disk, src_fmt="raw", dst_fmt="qcow2")

        log.info("Stopping backup check1")
        backup.stop_backup(qmp_client)

    log.info("Verifying backup")

    preview_disk = str(tmpdir.join("preview.qcow2"))
    qemu_img.create(preview_disk, "qcow2", backing=backup_disk)

    with qemu.run(preview_disk, "qcow2", qmp_sock) as guest:

        log.info("Logging in")
        guest.login("root", "")

        out = guest.run("ls -1 --color=never")
        assert out.splitlines() == ["before-backup"]
