# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from collections import namedtuple


class Extent(namedtuple("Extent", "start,length,zero")):
    """
    An image extent.

    Fields:
        start (int): offset in bytes.
        length (int): lenth in bytes.
        zero (bool): if True, this area will be read as zeroes.

    The extent describes either raw guest data, or raw host data, depending on
    the the backend. For example, file backend always reutrn host data, while
    NBD backend always return guest data.
    """
