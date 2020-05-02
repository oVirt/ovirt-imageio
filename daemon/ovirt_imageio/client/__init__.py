# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
client - smart client for accessing imageio server.
"""

# flake8: noqa

# Use for default buffer size.
from .. io import BUFFER_SIZE

# The public APIs
from . api import upload, download

# For better user experience.
from . ui import ProgressBar
