# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Manage imageio server.

This module exposes the public names. Anything else in this package is private
and should not be used.
"""

# flake8: noqa

from .. _internal import version

from .. _internal.server import (
    DEFAULT_CONF_DIR,
    load_config,
)

from . _api import (
    Client,
    ClientError,
    Error,
    ServerError,
)

__all__ = (
    "Client",
    "ClientError",
    "DEFAULT_CONF_DIR"
    "Error",
    "ServerError",
    "load_config",
)

__version__ = version.string
