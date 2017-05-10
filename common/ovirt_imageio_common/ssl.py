# ovirt-imageio-common
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import subprocess


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
