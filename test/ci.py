# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
Helpers for handling issues on specific CI environment.
"""

import os


def is_ovirt():
    return "OVIRT_CI" in os.environ


def is_travis():
    return "TRAVIS_CI" in os.environ
