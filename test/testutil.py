# ovirt-imageio
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
import logging
import socket
import subprocess

from contextlib import closing
from uuid import uuid4

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
                  sparse=None, dirty=None, inactivity_timeout=120):
    d = {
        "uuid": uuid or str(uuid4()),
        "timeout": timeout,
        "ops": ["read", "write"] if ops is None else ops,
        "size": size,
        "url": url,
        "transfer_id": transfer_id or str(uuid4()),
    }
    if filename is not None:
        d["filename"] = filename
    if sparse is not None:
        d["sparse"] = sparse
    if dirty is not None:
        d["dirty"] = dirty
    if inactivity_timeout is not None:
        d["inactivity_timeout"] = inactivity_timeout
    return d


def create_tempfile(tmpdir, name, data=b'', size=None):
    file = tmpdir.join(name)
    with open(str(file), 'wb') as f:
        if size is not None:
            f.truncate(size)
        if data:
            f.write(data)
    return file


def ipv6_enabled():
    out = subprocess.check_output(["ip", "-6", "-j", "addr"])
    addresses = json.loads(out)
    return len(addresses) > 0
