# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import subprocess

import pytest

from . import backup
from . import qemu_nbd


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_full_backup(tmpdir, fmt):
    disk_size = 1024**2
    disk_part = disk_size // 4
    disk = str(tmpdir.join("disk." + fmt))
    backup_disk = str(tmpdir.join("backup.raw"))

    # Create disk
    subprocess.check_call([
        "qemu-img",
        "create",
        "-f", fmt,
        disk,
        str(disk_size),
    ])

    # Pupulate disk with data.
    with qemu_nbd.open(disk, fmt) as d:
        for i in range(0, disk_size, disk_part):
            data = b"%d\n" % i
            d.write(i, data.ljust(512))
        d.flush()

    # Backup using qemu-img convert.
    with backup.full_backup(disk, fmt, tmpdir) as backup_url:
        subprocess.check_call([
            "qemu-img",
            "convert",
            "-p",
            "-f", "raw",
            "-O", "raw",
            backup_url,
            backup_disk,
        ])

    # Compare source and backup disks.
    with qemu_nbd.open(disk, fmt, read_only=True) as d, \
            io.open(backup_disk, "rb") as b:
        for i in range(0, disk_size, disk_part):
            b.seek(i)
            assert d.read(i, 512) == b.read(512)
