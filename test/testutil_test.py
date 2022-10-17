# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

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


def test_create_tempfile_hole(tmpdir):
    size = 1024
    file = testutil.create_tempfile(tmpdir, "image", size=size)
    assert file.read() == "\0" * size


def test_create_tempfile_data(tmpdir):
    size = 1024
    file = testutil.create_tempfile(tmpdir, "image", data=b"x" * size)
    assert file.read() == "x" * size


def test_create_tempfile_data_and_size(tmpdir):
    data_size = 512
    virtual_size = 1024
    file = testutil.create_tempfile(
        tmpdir, "image", data=b"x" * data_size, size=virtual_size)
    assert file.read() == "x" * data_size + "\0" * (virtual_size - data_size)
