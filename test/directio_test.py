# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import cStringIO
import pytest
from imaged import directio
from imaged import errors


BLOCK = "a" * directio.BLOCKSIZE
PARTIAL = "b" * 512
BYTES = "c" * 42


class param(str):
    """ Prevent pytest from showing the value in the test name """
    def __str__(self):
        return self[:10]


@pytest.mark.parametrize("data", [
    param(BLOCK * 2),
    param(BLOCK + PARTIAL * 2),
    param(BLOCK + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
    param(BYTES),
])
def test_copy_from_image(tmpdir, data):
    assert copy_from_image(tmpdir, data, len(data)) == data


@pytest.mark.parametrize(
    "size", [511, 513, len(BLOCK) + 511, len(BLOCK) + 513])
def test_copy_from_image_partial(tmpdir, size):
    data = BLOCK * 2
    assert copy_from_image(tmpdir, data, size) == data[:size]


@pytest.mark.parametrize("data", [
    param(BLOCK * 2),
    param(BLOCK + PARTIAL * 2),
    param(BLOCK + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
    param(BYTES),
])
def test_copy_from_image_partial_content(tmpdir, data):
    with pytest.raises(errors.PartialContent) as e:
        copy_from_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def copy_from_image(tmpdir, data, size):
    src = tmpdir.join("src")
    src.write(data)
    dst = cStringIO.StringIO()
    directio.copy_from_image(str(src), dst, size)
    return dst.getvalue()


@pytest.mark.parametrize("data", [
    param(BLOCK * 2),
    param(BLOCK + PARTIAL * 2),
    param(BLOCK + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
    param(BYTES),
])
def test_copy_to_image(tmpdir, data):
    assert copy_to_image(tmpdir, data, len(data)) == data


@pytest.mark.parametrize(
    "size", [511, 513, len(BLOCK) + 511, len(BLOCK) + 513])
def test_copy_to_image_partial(tmpdir, size):
    data = BLOCK * 2
    assert copy_to_image(tmpdir, data, size) == data[:size]


@pytest.mark.parametrize("data", [
    param(BLOCK * 2),
    param(BLOCK + PARTIAL * 2),
    param(BLOCK + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
    param(BYTES),
])
def test_copy_to_image_partial_content(tmpdir, data):
    with pytest.raises(errors.PartialContent) as e:
        copy_to_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def copy_to_image(tmpdir, data, size):
    dst = tmpdir.join("dst")
    dst.write("")
    src = cStringIO.StringIO(data)
    directio.copy_to_image(str(dst), src, size)
    return dst.read()
