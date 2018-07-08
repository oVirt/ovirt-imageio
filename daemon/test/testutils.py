# ovirt-imageio-daemon
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import uuid


def create_ticket(uuid=str(uuid.uuid4()), ops=None, timeout=300, size=2**64,
                  url="file:///var/run/vdsm/storage/foo", filename=None):
    d = {
        "uuid": uuid,
        "timeout": timeout,
        "ops": ["read", "write"] if ops is None else ops,
        "size": size,
        "url": url,
    }
    if filename is not None:
        d["filename"] = filename
    return d


def create_tempfile(tmpdir, name, data='', size=None):
    file = tmpdir.join(name)
    with open(str(file), 'wb') as f:
        if size is not None:
            f.truncate(size)
        if data:
            f.write(data)
    return file
