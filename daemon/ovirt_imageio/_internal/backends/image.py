# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from collections import namedtuple


class ZeroExtent(namedtuple("ZeroExtent", "start,length,zero")):
    """
    An image extent describing image data.

    Fields:
        start (int): offset in bytes.
        length (int): lenth in bytes.
        zero (bool): if True, this area will be read as zeroes.

    The extent describes either raw guest data, or raw host data, depending on
    the the backend. For example, file backend always reutrn host data, while
    NBD backend always return guest data.
    """


class DirtyExtent(namedtuple("DirtyExtent", "start,length,dirty")):
    """
    An image extent describing dirty areas that have changed since a previous
    checkpoint. This information is available during incremental backup.

    Fields:
        start (int): offset in bytes.
        length (int): lenth in bytes.
        dirty (bool): if True, this area was changed since the last checkpoint.

    The extent always describes raw guest data.
    """
