# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import socket

from contextlib import closing

from . import testutil


def test_random_tcp_port():
    # Use 100 iterations to detect flakyness early.
    for i in range(100):
        s = socket.socket()
        with closing(s):
            port = testutil.random_tcp_port()
            s.bind(("localhost", port))
