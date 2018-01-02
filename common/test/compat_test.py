# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import io

import six
import pytest

from ovirt_imageio_common import compat
from ovirt_imageio_common import directio

require_py3 = pytest.mark.skipif(six.PY2, reason="Requires python 3")

buffer_types = pytest.mark.parametrize("buftype", [
    bytearray,
    pytest.param(directio.aligned_buffer, id="aligned_buffer")
])


@buffer_types
def test_bufview_readonly(buftype):
    buf = buftype(30)
    buf[:] = b"a" * 10 + b"b" * 10 + b"c" * 10
    view = compat.bufview(buf, 10, 10)
    assert view[:] == b"b" * 10


@buffer_types
def test_bufview_write(buftype):
    buf = buftype(30)
    buf[:] = b"a" * 10 + b"b" * 10 + b"c" * 10
    view = compat.bufview(buf, 10, 10)
    w = io.BytesIO()
    w.write(view)
    assert w.getvalue() == b"b" * 10


@require_py3
@buffer_types
def test_bufview_readwrite(buftype):
    buf = buftype(30)
    buf[:] = b"a" * 30
    view = compat.bufview(buf, 10, 10)
    view[:] = b"b" * 10
    assert buf[:] == b"a" * 10 + b"b" * 10 + b"a" * 10


@require_py3
@buffer_types
def test_bufview_readinto(buftype):
    buf = buftype(30)
    buf[:] = b"a" * 30
    r = io.BytesIO(b"b" * 20)
    view = compat.bufview(buf, 10, 10)
    r.readinto(view)
    assert buf[:] == b"a" * 10 + b"b" * 10 + b"a" * 10
