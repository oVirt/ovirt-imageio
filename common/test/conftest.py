# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io
import pytest
from six.moves import urllib_parse

from ovirt_imageio_common import nbd

from . import qemu_nbd


@pytest.fixture
def tmpfile(tmpdir):
    """
    Return a path to an empty temporary file.
    """
    f = tmpdir.join("tmpfile")
    f.write("")
    return str(f)


@pytest.fixture
def tmpurl(tmpfile):
    """
    Return a file: url to an empty temporary file.
    """
    return urllib_parse.urlparse("file:" + tmpfile)


@pytest.fixture
def nbd_server(tmpdir):
    """
    Returns nbd_server exporting a temporary file.

    The test may configure the server before starting it. Typical example is
    setting the read_only flag to create a read only server.

    The test must start the server. The test framework will stop the server
    when the test ends.
    """
    image = str(tmpdir.join("image"))
    with io.open(image, "wb") as f:
        f.truncate(10 * 1024**2)

    sock = nbd.UnixAddress(tmpdir.join("sock"))

    server = qemu_nbd.Server(image, "raw", sock)
    try:
        yield server
    finally:
        server.stop()
