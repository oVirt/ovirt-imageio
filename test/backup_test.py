# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import io
import logging

import pytest

from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd

from . import backup
from . import ci
from . import distro
from . import qemu
from . import qmp
from . import testutil

from . marks import flaky_in_ovirt_ci

log = logging.getLogger("test")


@pytest.mark.parametrize("transport", [
    "unix",
    pytest.param("tcp", marks=flaky_in_ovirt_ci),
])
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
    ci.is_ovirt() or distro.is_centos("9"),
    reason="Always times out", run=False)
def test_full_backup_guest(tmpdir, base_image):
    base = qemu_img.info(base_image)
    disk_size = base["virtual-size"]

    disk = str(tmpdir.join("disk.qcow2"))
    qemu_img.create(
        disk, "qcow2", backing_file=base_image, backing_format=base["format"])

    scratch_disk = str(tmpdir.join("scratch.qcow2"))
    qemu_img.create(scratch_disk, "qcow2", size=disk_size)

    backup_disk = str(tmpdir.join("backup.qcow2"))
    qemu_img.create(backup_disk, "qcow2", size=disk_size)

    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))
    nbd_sock = nbd.UnixAddress(tmpdir.join("nbd.sock"))

    with qemu.run(disk, "qcow2", qmp_sock, shutdown_timeout=10) as guest, \
            qmp.Client(qmp_sock) as qmp_client:
        guest.login("root", "")

        assert guest.run("touch before-backup; sync") == ""

        with backup.run(
                qmp_client, nbd_sock, scratch_disk, checkpoint="check1"):

            assert guest.run("touch during-backup; sync") == ""

            backup.copy_disk(nbd_sock.url("sda"), backup_disk)

    verify_backup(backup_disk, ["before-backup"])


@pytest.mark.timeout(120)
@pytest.mark.xfail(
    ci.is_ovirt() or distro.is_centos("9"),
    reason="Always times out", run=False)
def test_incremental_backup_guest(tmpdir, base_image):
    base = qemu_img.info(base_image)
    disk_size = base["virtual-size"]

    disk = str(tmpdir.join("disk.qcow2"))
    qemu_img.create(
        disk, "qcow2", backing_file=base_image, backing_format=base["format"])

    scratch_disk = str(tmpdir.join("scratch.qcow2"))
    qemu_img.create(scratch_disk, "qcow2", size=disk_size)

    full_backup_disk = str(tmpdir.join("full-backup.qcow2"))
    qemu_img.create(full_backup_disk, "qcow2", size=disk_size)

    incr_backup_disk = str(tmpdir.join("incr-backup.qcow2"))
    qemu_img.create(incr_backup_disk, "qcow2", size=disk_size)

    qmp_sock = nbd.UnixAddress(tmpdir.join("qmp.sock"))
    nbd_sock = nbd.UnixAddress(tmpdir.join("nbd.sock"))

    with qemu.run(disk, "qcow2", qmp_sock, shutdown_timeout=10) as guest, \
            qmp.Client(qmp_sock) as qmp_client:
        guest.login("root", "")

        with backup.run(
                qmp_client, nbd_sock, scratch_disk, checkpoint="check1"):

            backup.copy_disk(nbd_sock.url("sda"), full_backup_disk)

        qemu_img.create(scratch_disk, "qcow2", size=disk_size)

        assert guest.run("touch before-backup; sync") == ""

        with backup.run(
                qmp_client, nbd_sock, scratch_disk, checkpoint="check2",
                incremental="check1"):

            assert guest.run("touch during-backup; sync") == ""

            backup.copy_dirty(nbd_sock.url("sda"), incr_backup_disk)

    qemu_img.unsafe_rebase(incr_backup_disk, full_backup_disk, "qcow2")
    verify_backup(incr_backup_disk, ["before-backup"])


def verify_backup(backup_disk, expected_files):
    log.info("Verifying backup")

    preview_disk = backup_disk + ".preview"
    qemu_img.create(
        preview_disk,
        "qcow2",
        backing_file=backup_disk,
        backing_format="qcow2")

    with qemu.run(preview_disk, "qcow2") as guest:
        guest.login("root", "")
        out = guest.run("ls -1 --color=never")
        assert out.splitlines() == expected_files
