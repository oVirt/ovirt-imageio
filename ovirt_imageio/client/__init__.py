# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
Smart client for accessing imageio server.

This module exposes the public names. Anything else in this package is private
and should not be used.
"""

# flake8: noqa

# Use for default buffer size.
from .. _internal.io import BUFFER_SIZE

from .. _internal import version

# The public APIs
from . _api import (
    upload,
    download,
    info,
    measure,
    checksum,
    extents,
    ImageioClient,
)

# For better user experience.
from . _ui import ProgressBar

__all__ = (
    "BUFFER_SIZE",
    "ImageioClient",
    "ProgressBar",
    "checksum",
    "download",
    "extents",
    "info",
    "measure",
    "upload",
)

__version__ = version.string
