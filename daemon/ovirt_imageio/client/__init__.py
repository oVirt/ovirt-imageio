# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
client - smart client for accessing imageio server.

This module exposes the public names. Anything else in this package is private
and should not be used.
"""

# flake8: noqa

# Use for default buffer size.
from .. _internal.io import BUFFER_SIZE

# The public APIs
from . _api import upload, download

# For better user experience.
from . _ui import ProgressBar
