# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Compatibility module for vdsm.

vdsm uses ovirt_imageio_common.directio.Receive. We plan to remove the
dependency, but we cannot fix old vdsm versions.

Changes to this module must be tested with /usr/libexec/vdsm/kvm2ovirt.
"""

from __future__ import absolute_import
from . ops import Receive  # NOQA: F401 (unused import)
