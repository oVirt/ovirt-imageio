# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from ovirt_imageio import client
from ovirt_imageio._internal.units import MiB, GiB


class FakeTime:

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FakeFile:

    def __init__(self):
        self.last = None

    def write(self, s):
        self.last = s

    def flush(self):
        pass


def test_draw():
    fake_time = FakeTime()
    f = FakeFile()

    # Size is unknown at this point.
    pb = client.ProgressBar(output=f, now=fake_time)
    line = "[ ---- ] 0 bytes, 0.00 seconds, 0 bytes/s"
    assert f.last == line.ljust(79) + "\r"

    # Size was updated, but no bytes were transferred yet.
    fake_time.now += 0.1
    pb.size = 3 * GiB
    pb.update(0)
    line = "[   0% ] 0 bytes, 0.10 seconds, 0 bytes/s"
    assert f.last == line.ljust(79) + "\r"

    # Write some data...
    fake_time.now += 1.0
    pb.update(512 * MiB)
    line = "[  16% ] 512.00 MiB, 1.10 seconds, 465.45 MiB/s"
    assert f.last == line.ljust(79) + "\r"

    # Write zeros (much faster)...
    fake_time.now += 0.2
    pb.update(2 * GiB)
    line = "[  83% ] 2.50 GiB, 1.30 seconds, 1.92 GiB/s"
    assert f.last == line.ljust(79) + "\r"

    # More data, slow down again...
    fake_time.now += 1.0
    pb.update(512 * MiB)
    line = "[ 100% ] 3.00 GiB, 2.30 seconds, 1.30 GiB/s"
    assert f.last == line.ljust(79) + "\r"

    # Flush takes some time, lowering final rate.
    fake_time.now += 0.1
    pb.close()
    line = "[ 100% ] 3.00 GiB, 2.40 seconds, 1.25 GiB/s"
    assert f.last == line.ljust(79) + "\n"


def test_with_size():
    fake_time = FakeTime()
    f = FakeFile()

    client.ProgressBar(size=3 * GiB, output=f, now=fake_time)
    line = "[   0% ] 0 bytes, 0.00 seconds, 0 bytes/s"
    assert f.last == line.ljust(79) + "\r"


def test_close():
    f = FakeFile()
    pb = client.ProgressBar(output=f)
    pb.size = 1 * GiB
    pb.update(512 * MiB)
    pb.close()

    # Once closed, update does not redraw.
    f.last = None
    pb.update(512 * MiB)
    assert f.last is None

    # Closing twice does not redraw.
    pb.close()
    assert f.last is None


def test_contextmanager():
    f = FakeFile()
    with client.ProgressBar(GiB, output=f) as pb:
        pb.update(GiB)
        assert f.last.endswith("\r")

    assert f.last.endswith("\n")
