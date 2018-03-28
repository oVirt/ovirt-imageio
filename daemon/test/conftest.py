# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Shared pytest fixtures
"""

import pytest

from ovirt_imageio_common import util


@pytest.fixture
def tmpfile(tmpdir):
    """
    Return an empty file inside a temporary test directory.
    """
    f = tmpdir.join("tmpfile")
    f.write("")
    return f


class FakeTime(object):

    def __init__(self):
        self.now = 0

    def monotonic_time(self):
        return self.now


@pytest.fixture
def fake_time(monkeypatch):
    """
    Monkeypatch util.monotonic_time for testing time related operations.

    Returns FakeTime instance. Modifying instance.now change the value returned
    from the monkeypatched util.monotonic_time().
    """
    time = FakeTime()
    monkeypatch.setattr(util, "monotonic_time", time.monotonic_time)
    return time
