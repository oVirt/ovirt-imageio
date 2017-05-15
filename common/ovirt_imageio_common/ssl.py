# ovirt-imageio-common
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import ssl
import subprocess


def server_context(cafile, certfile, keyfile):
    # TODO: Verify client certs
    return _context(ssl.Purpose.CLIENT_AUTH, cafile, certfile, keyfile)


def client_context(cafile, certfile, keyfile):
    return _context(ssl.Purpose.SERVER_AUTH, cafile, certfile, keyfile)


def _context(purpose, cafile, certfile, keyfile):
    ctx = ssl.create_default_context(purpose=purpose, cafile=cafile)
    ctx.options |= ssl.OP_NO_TLSv1
    ctx.load_cert_chain(certfile, keyfile)
    return ctx


def check_protocol(server_host, server_port, protocol):
    """
    Use openssl command line tool for checking ssl protocol.
    """
    # TODO: Use cert to allow client cert verfication
    cmd = ["openssl",
           "s_client",
           "-connect", "%s:%d" % (server_host, server_port),
           protocol]
    return subprocess.call(cmd)
