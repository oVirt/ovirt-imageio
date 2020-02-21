# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest
import six

from . import distro

xfail_python3 = pytest.mark.xfail(six.PY3, reason="Needs porting to python 3")

requires_advanced_virt = pytest.mark.skipif(
    distro.is_centos("8"),
    reason="Advanced virt stream not available")


requires_python3 = pytest.mark.skipif(six.PY2, reason="Requires python 3")
