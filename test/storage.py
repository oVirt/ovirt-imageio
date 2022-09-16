# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import urllib.parse


class Backend:
    """
    Wrap a userstorage.Backend, adding a url and the can_detect_sector_size
    attribute.
    """

    def __init__(self, storage, can_detect_sector_size=True):
        self._storage = storage
        self.url = urllib.parse.urlparse("file:" + storage.path)
        self.can_detect_sector_size = can_detect_sector_size

    @property
    def path(self):
        return self._storage.path

    @property
    def sector_size(self):
        return self._storage.sector_size

    def __enter__(self):
        self._storage.__enter__()
        return self

    def __exit__(self, *args):
        self._storage.__exit__(*args)
