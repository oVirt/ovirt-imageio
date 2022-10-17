# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Compatibility module for vdsm.

vdsm uses ovirt_imageio_common.directio.Receive. We plan to remove the
dependency, but we cannot fix old vdsm versions.

Changes to this module must be tested with /usr/libexec/vdsm/kvm2ovirt.
"""

import urllib.parse as urllib_parse

from . import ops
from . import stats
from . import util
from . backends import file
from .units import MiB


class Receive(ops.Write):
    """
    ops.Write accepts a backend instead of path, and buffer instead of
    buffersize. Provide a backward compatible version for existing users (e.g.
    vdsm).
    """

    def __init__(self, path, src, size=None, offset=0, flush=True,
                 buffersize=MiB, clock=stats.NullClock()):
        url = urllib_parse.urlparse("file:" + path)
        self._dst = file.open(url, "r+")
        buf = util.aligned_buffer(buffersize)
        super().__init__(self._dst, src, buf, size=size, offset=offset,
                         flush=flush, clock=clock)

    def close(self):
        if self._dst:
            try:
                self._dst.close()
            finally:
                self._dst = None
            try:
                self._buf.close()
            finally:
                self._buf = None
            super().close()
