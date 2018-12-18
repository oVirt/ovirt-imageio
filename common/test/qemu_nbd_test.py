# ovirt-diskio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import subprocess
import pytest

from . import qemu_nbd


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_open(tmpdir, fmt):
    disk = str(tmpdir.join("disk." + fmt))

    subprocess.check_call([
        "qemu-img",
        "create",
        "-f", fmt,
        disk,
        "1m",
    ])

    offset = 64 * 1024
    data = b"it works"

    with qemu_nbd.open(disk, fmt) as d:
        d.write(offset, data)
        d.flush()

    with qemu_nbd.open(disk, fmt, read_only=True) as d:
        assert d.read(offset, len(data)) == data
