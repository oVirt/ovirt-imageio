# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

from ovirt_imageio._internal import errors


def test_str():
    e = errors.PartialContent(50, 42)
    assert str(e) == "Requested 50 bytes, available 42 bytes"
