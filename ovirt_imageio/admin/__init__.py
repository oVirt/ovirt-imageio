# ovirt-imageio
# Copyright (C) 2021 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
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
