# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest

from ovirt_imageio_common import ui


class FakeTime(object):

    def __init__(self):
        self.now = 0.0

    def time(self):
        return self.now


class FakeFile(object):

    def __init__(self):
        self.last = None

    def write(self, s):
        self.last = s

    def flush(self):
        pass


def test_draw(monkeypatch):
    monkeypatch.setattr(ui, "time", FakeTime())
    f = FakeFile()

    # Size is unknown at this point.
    pb = ui.ProgressBar(output=f, step=0.1)
    assert f.last == (
        "[ ------- ] 0 bytes, 0.00 seconds, 0 bytes/s".ljust(79) + "\r"
    )

    # Size was updated.
    ui.time.now += 0.1
    pb.size = 3 * 1024**3
    pb.update(0)
    assert f.last == (
        "[   0.00% ] 0 bytes, 0.10 seconds, 0 bytes/s".ljust(79) + "\r"
    )

    # Write some data...
    ui.time.now += 1.0
    pb.update(512 * 1024**2)
    assert f.last == (
        "[  16.67% ] 512.00 MiB, 1.10 seconds, 465.45 MiB/s".ljust(79) + "\r"
    )

    # Write zeros, much faster, but it is not time to update yet...
    ui.time.now += 0.05
    pb.update(512 * 1024**2)
    assert f.last == (
        "[  16.67% ] 512.00 MiB, 1.10 seconds, 465.45 MiB/s".ljust(79) + "\r"
    )

    # Write more zeors, time to update...
    ui.time.now += 0.05
    pb.update(512 * 1024**2)
    assert f.last == (
        "[  50.00% ] 1.50 GiB, 1.20 seconds, 1.25 GiB/s".ljust(79) + "\r"
    )

    # More zeros, rates increases...
    ui.time.now += 0.1
    pb.update(1024**3)
    assert f.last == (
        "[  83.33% ] 2.50 GiB, 1.30 seconds, 1.92 GiB/s".ljust(79) + "\r"
    )

    # More data, slow down again...
    ui.time.now += 1.0
    pb.update(512 * 1024**2)
    assert f.last == (
        "[ 100.00% ] 3.00 GiB, 2.30 seconds, 1.30 GiB/s".ljust(79) + "\r"
    )

    # Flush takes some time, lowering final rate.
    ui.time.now += 0.1
    pb.close()
    assert f.last == (
        "[ 100.00% ] 3.00 GiB, 2.40 seconds, 1.25 GiB/s".ljust(79) + "\n"
    )


def test_contextmanager():
    f = FakeFile()
    with ui.ProgressBar(1024**3, output=f) as pb:
        pb.update(1024**3)
        assert f.last.endswith("\r")

    assert f.last.endswith("\n")


@pytest.mark.parametrize("n,s", [
    (0, "0 bytes"),
    (0.0, "0 bytes"),
    (1023, "1023 bytes"),
    (1024, "1.00 KiB"),
    (1024 * 1023, "1023.00 KiB"),
    (1024 * 1024, "1.00 MiB"),
    (1024**2 * 1023, "1023.00 MiB"),
    (1024**2 * 1024, "1.00 GiB"),
    (1024**3 * 1023, "1023.00 GiB"),
    (1024**3 * 1024, "1.00 TiB"),
    (1024**4 * 1023, "1023.00 TiB"),
    (1024**4 * 1024, "1.00 PiB"),
])
def test_humansize(n, s):
    assert ui.humansize(n) == s
