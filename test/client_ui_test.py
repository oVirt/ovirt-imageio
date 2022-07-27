# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

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
    pb = client.ProgressBar(phase="setting up", output=f, now=fake_time)
    line = "[ ---- ] 0 bytes, 0.00 s, 0 bytes/s | setting up"
    assert f.last == line.ljust(79) + "\r"

    # Size was updated, but no bytes were transferred yet.
    fake_time.now += 0.1
    pb.size = 3 * GiB
    line = "[   0% ] 0 bytes, 0.10 s, 0 bytes/s | setting up"
    assert f.last == line.ljust(79) + "\r"

    # Phase was updated.
    fake_time.now += 0.2
    pb.phase = "downloading image"
    line = "[   0% ] 0 bytes, 0.30 s, 0 bytes/s | downloading image"
    assert f.last == line.ljust(79) + "\r"

    # Write some data...
    fake_time.now += 0.8
    pb.update(512 * MiB)
    line = "[  16% ] 512.00 MiB, 1.10 s, 465.45 MiB/s | downloading image"
    assert f.last == line.ljust(79) + "\r"

    # Write zeros (much faster)...
    fake_time.now += 0.2
    pb.update(2 * GiB)
    line = "[  83% ] 2.50 GiB, 1.30 s, 1.92 GiB/s | downloading image"
    assert f.last == line.ljust(79) + "\r"

    # More data, slow down again...
    fake_time.now += 1.0
    pb.update(512 * MiB)
    line = "[ 100% ] 3.00 GiB, 2.30 s, 1.30 GiB/s | downloading image"
    assert f.last == line.ljust(79) + "\r"

    # Cleaning up after download.
    pb.phase = "cleaning up"
    line = "[ 100% ] 3.00 GiB, 2.30 s, 1.30 GiB/s | cleaning up"
    assert f.last == line.ljust(79) + "\r"

    # Cleaning can take few seconds, lowwing the the rate.
    fake_time.now += 3.0
    pb.phase = "download completed"
    line = "[ 100% ] 3.00 GiB, 5.30 s, 579.62 MiB/s | download completed"
    assert f.last == line.ljust(79) + "\r"

    # Closing prints the final line with a newline terminator.
    pb.close()
    line = "[ 100% ] 3.00 GiB, 5.30 s, 579.62 MiB/s | download completed"
    assert f.last == line.ljust(79) + "\n"


def test_with_size():
    fake_time = FakeTime()
    f = FakeFile()

    client.ProgressBar(phase="starting", size=3 * GiB, output=f, now=fake_time)
    line = "[   0% ] 0 bytes, 0.00 s, 0 bytes/s | starting"
    assert f.last == line.ljust(79) + "\r"


def test_without_phase():
    fake_time = FakeTime()
    f = FakeFile()

    client.ProgressBar(output=f, now=fake_time)
    line = "[ ---- ] 0 bytes, 0.00 s, 0 bytes/s"
    assert f.last == line.ljust(79) + "\r"


def test_close():
    f = FakeFile()
    pb = client.ProgressBar(output=f)
    pb.size = 1 * GiB
    pb.update(512 * MiB)
    pb.close()
    f.last = None

    # Once closed, update does not redraw.
    pb.update(512 * MiB)
    assert f.last is None

    # Changing size does nothing.
    pb.size = 2 * GiB
    assert f.last is None
    assert pb.size == 1 * GiB

    # Changing phase does nothing.
    pb.phase = "new phase"
    assert f.last is None
    assert pb.phase is None

    # Closing twice does not redraw.
    pb.close()
    assert f.last is None


def test_contextmanager():
    f = FakeFile()
    with client.ProgressBar(size=GiB, output=f) as pb:
        pb.update(GiB)
        assert f.last.endswith("\r")

    assert f.last.endswith("\n")


def test_error_phase_default():
    f = FakeFile()
    pb = client.ProgressBar(phase="running command", size=GiB, output=f)
    with pytest.raises(RuntimeError):
        with pb:
            raise RuntimeError

    assert f.last.endswith("\n")
    assert pb.phase == "command failed"


def test_error_phase_custom():
    f = FakeFile()
    pb = client.ProgressBar(
        phase="starting operation",
        error_phase="operation failed",
        size=GiB,
        output=f)
    with pytest.raises(RuntimeError):
        with pb:
            raise RuntimeError

    assert f.last.endswith("\n")
    assert pb.phase == "operation failed"
