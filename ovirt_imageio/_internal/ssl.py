# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import ssl
import subprocess


def server_context(certfile, keyfile, cafile=None, enable_tls1_1=False):
    # TODO: Verify client certs
    ctx = ssl.create_default_context(
        purpose=ssl.Purpose.CLIENT_AUTH, cafile=cafile)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_1
    if not enable_tls1_1:
        ctx.minimum_version = ssl.TLSVersion.TLSv1
    ctx.load_cert_chain(certfile, keyfile=keyfile)
    return ctx


def client_context(cafile=None, enable_tls1_1=False):
    ctx = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_1
    if not enable_tls1_1:
        ctx.minimum_version = ssl.TLSVersion.TLSv1
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
