# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Helpers for handling issues on specific CI environment.
"""

import os


def is_ovirt():
    return "OVIRT_CI" in os.environ


def is_travis():
    return "TRAVIS_CI" in os.environ
