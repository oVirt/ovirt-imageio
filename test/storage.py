# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

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
