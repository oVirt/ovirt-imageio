# ovirt-imageio
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import logging
import socket
from uuid import uuid4

from contextlib import closing

log = logging.getLogger("test")


def head(b):
    return b[:10]


def random_tcp_port():
    """
    Find a random (likely) unused port.

    The port is unused when the call return, but another process may
    grab it.  If you don't control the environmemnt, be prepared for
    bind failures.
    """
    s = socket.socket()
    with closing(s):
        s.bind(("localhost", 0))
        port = s.getsockname()[1]
        log.debug("Found unused TCP port %s", port)
        return port


def create_ticket(uuid=None, ops=None, timeout=300, size=2**64,
                  url="file:///tmp/foo.img", transfer_id=None, filename=None,
                  sparse=None, dirty=None):
    d = {
        "uuid": uuid or str(uuid4()),
        "timeout": timeout,
        "ops": ["read", "write"] if ops is None else ops,
        "size": size,
        "url": url,
    }
    if transfer_id is not None:
        d["transfer_id"] = transfer_id
    if filename is not None:
        d["filename"] = filename
    if sparse is not None:
        d["sparse"] = sparse
    if dirty is not None:
        d["dirty"] = dirty
    return d


def create_tempfile(tmpdir, name, data=b'', size=None):
    file = tmpdir.join(name)
    with open(str(file), 'wb') as f:
        if size is not None:
            f.truncate(size)
        if data:
            f.write(data)
    return file


def ipv6_enabled(dev='default'):
    # Based on
    # https://github.com/oVirt/vdsm/blob/v4.40.19/lib/vdsm/network/sysctl.py#L59
    try:
        with open('/proc/sys/net/ipv6/conf/%s/disable_ipv6' % dev) as f:
            return bool(int(f.read()))
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
        return False
