# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest


@pytest.fixture
def tmpfile(tmpdir):
    f = tmpdir.join("tmpfile")
    f.write(b"x" * 4096)
    return f
