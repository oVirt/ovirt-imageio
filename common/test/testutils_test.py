# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import
from __future__ import print_function

from . import testutils


def test_create_tempfile_hole(tmpdir):
    size = 1024
    file = testutils.create_tempfile(tmpdir, "image", size=size)
    assert file.read() == "\0" * size


def test_create_tempfile_data(tmpdir):
    size = 1024
    file = testutils.create_tempfile(tmpdir, "image", data=b"x" * size)
    assert file.read() == "x" * size


def test_create_tempfile_data_and_size(tmpdir):
    data_size = 512
    virtual_size = 1024
    file = testutils.create_tempfile(
        tmpdir, "image", data=b"x" * data_size, size=virtual_size)
    assert file.read() == "x" * data_size + "\0" * (virtual_size - data_size)
