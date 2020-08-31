# ovirt-imageio
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import ssl
import subprocess


def server_context(certfile, keyfile, cafile=None, enable_tls1_1=False):
    # TODO: Verify client certs
    ctx = ssl.create_default_context(
        purpose=ssl.Purpose.CLIENT_AUTH, cafile=cafile)
    ctx.options |= ssl.OP_NO_TLSv1
    if not enable_tls1_1:
        ctx.options |= ssl.OP_NO_TLSv1_1
    ctx.load_cert_chain(certfile, keyfile=keyfile)
    return ctx


def client_context(cafile=None, enable_tls1_1=False):
    ctx = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.options |= ssl.OP_NO_TLSv1
    if not enable_tls1_1:
        ctx.options |= ssl.OP_NO_TLSv1_1
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
