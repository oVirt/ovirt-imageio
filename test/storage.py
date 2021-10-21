# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import urllib.parse

import pytest


class Backend:
    """
    Wrap a userstorage.Backend, adding a url and context manager interface to
    simplify fixtures.
    """

    def __init__(self, storage, can_detect_sector_size=True):
        if not storage.exists():
            pytest.xfail("Storage {} is not available".format(storage.name))

        self._storage = storage
        self.path = storage.path
        self.url = urllib.parse.urlparse("file:" + storage.path)
        self.sector_size = storage.sector_size
        self.can_detect_sector_size = can_detect_sector_size

    def __enter__(self):
        self._storage.setup()
        return self

    def __exit__(self, *args):
        self._storage.teardown()
