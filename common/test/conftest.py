# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import pytest

from six.moves import urllib_parse


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
