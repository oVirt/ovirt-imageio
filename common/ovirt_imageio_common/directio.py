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

import urllib.parse as urllib_parse

from . import ops
from . import util
from . backends import file


class Receive(ops.Receive):
    """
    ops.Receive accepts a backend instead of path, but vdsm is using the old
    interface. Provide a backward comptible version accepting a path.
    """

    def __init__(self, path, src, size=None, offset=0, flush=True,
                 buffersize=ops.BUFFERSIZE, clock=util.NullClock()):
        url = urllib_parse.urlparse("file:" + path)
        self._dst = file.open(url, "r+")
        super(Receive, self).__init__(self._dst, src, size=size, offset=offset,
                                      flush=flush, buffersize=buffersize,
                                      clock=clock)

    def close(self):
        if self._dst:
            try:
                self._dst.close()
            finally:
                self._dst = None
            super(Receive, self).close()
