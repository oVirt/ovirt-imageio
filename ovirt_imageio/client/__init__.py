# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Smart client for accessing imageio server.

This module exposes the public names. Anything else in this package is private
and should not be used.
"""

# flake8: noqa

from .. _internal import version

# The public APIs
from . _api import (
    BUFFER_SIZE,
    MAX_WORKERS,
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
    "MAX_WORKERS",
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
