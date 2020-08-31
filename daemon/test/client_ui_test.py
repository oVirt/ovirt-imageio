# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from ovirt_imageio import client


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
    pb = client.ProgressBar(output=f, step=0.1, now=fake_time)
    assert f.last == (
        "[ ------- ] 0 bytes, 0.00 seconds, 0 bytes/s".ljust(79) + "\r"
    )

    # Size was updated.
    fake_time.now += 0.1
    pb.size = 3 * 1024**3
    pb.update(0)
    assert f.last == (
        "[   0.00% ] 0 bytes, 0.10 seconds, 0 bytes/s".ljust(79) + "\r"
    )

    # Write some data...
    fake_time.now += 1.0
    pb.update(512 * 1024**2)
    assert f.last == (
        "[  16.67% ] 512.00 MiB, 1.10 seconds, 465.45 MiB/s".ljust(79) + "\r"
    )

    # Write zeros, much faster, but it is not time to update yet...
    fake_time.now += 0.05
    pb.update(512 * 1024**2)
    assert f.last == (
        "[  16.67% ] 512.00 MiB, 1.10 seconds, 465.45 MiB/s".ljust(79) + "\r"
    )

    # Write more zeors, time to update...
    fake_time.now += 0.05
    pb.update(512 * 1024**2)
    assert f.last == (
        "[  50.00% ] 1.50 GiB, 1.20 seconds, 1.25 GiB/s".ljust(79) + "\r"
    )

    # More zeros, rates increases...
    fake_time.now += 0.1
    pb.update(1024**3)
    assert f.last == (
        "[  83.33% ] 2.50 GiB, 1.30 seconds, 1.92 GiB/s".ljust(79) + "\r"
    )

    # More data, slow down again...
    fake_time.now += 1.0
    pb.update(512 * 1024**2)
    assert f.last == (
        "[ 100.00% ] 3.00 GiB, 2.30 seconds, 1.30 GiB/s".ljust(79) + "\r"
    )

    # Flush takes some time, lowering final rate.
    fake_time.now += 0.1
    pb.close()
    assert f.last == (
        "[ 100.00% ] 3.00 GiB, 2.40 seconds, 1.25 GiB/s".ljust(79) + "\n"
    )


def test_contextmanager():
    f = FakeFile()
    with client.ProgressBar(1024**3, output=f) as pb:
        pb.update(1024**3)
        assert f.last.endswith("\r")

    assert f.last.endswith("\n")
