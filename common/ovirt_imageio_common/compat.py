# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
compat - python compatibility helpers
"""

import six

if six.PY2:
    def bufview(buf, pos, size):
        return buffer(buf, pos, size)
else:
    def bufview(buf, pos, size):
        return memoryview(buf)[pos:pos + size]

if six.PY2:
    import subprocess32 as subprocess
else:
    import subprocess  # noqa: F401
