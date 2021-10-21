# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

from . import ci
from . import testutil

requires_ipv6 = pytest.mark.skipif(
    not testutil.ipv6_enabled(),
    reason="IPv6 not available")

flaky_in_ovirt_ci = pytest.mark.xfail(
    ci.is_ovirt(), reason="Test is flaky in oVirt CI", strict=False)
