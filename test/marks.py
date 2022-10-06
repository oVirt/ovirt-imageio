# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import pytest

from . import ci
from . import testutil

requires_ipv6 = pytest.mark.skipif(
    not testutil.ipv6_enabled(),
    reason="IPv6 not available")

flaky_in_ovirt_ci = pytest.mark.xfail(
    ci.is_ovirt(), reason="Test is flaky in oVirt CI", strict=False)
