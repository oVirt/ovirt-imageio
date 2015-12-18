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


def test_copy_from_image_full_blocks(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2
    assert copy_from_image(tmpdir, data, len(data)) == data


def test_copy_from_image_full_blocks_partial_content(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2
    with pytest.raises(errors.PartialContent) as e:
        copy_from_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_from_image_full_blocks_and_partial_block(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3
    assert copy_from_image(tmpdir, data, len(data)) == data


def test_copy_from_image_full_blocks_and_partial_block_partial_content(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3
    with pytest.raises(errors.PartialContent) as e:
        copy_from_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_full_block_and_partial_and_some(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3 + "c" * 42
    assert copy_from_image(tmpdir, data, len(data)) == data


def test_copy_to_image_full_block_and_partial_and_some_partial_content(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3 + "c" * 42
    with pytest.raises(errors.PartialContent) as e:
        copy_from_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_partial_block(tmpdir):
    data = "a" * 512 * 3
    assert copy_from_image(tmpdir, data, len(data)) == data


def test_copy_to_image_partial_block_partial_content(tmpdir):
    data = "a" * 512 * 3
    with pytest.raises(errors.PartialContent) as e:
        copy_from_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_partial_block_and_then_some(tmpdir):
    data = "a" * 512 * 3 + "b" * 42
    assert copy_from_image(tmpdir, data, len(data)) == data


def test_copy_to_image_partial_block_and_then_some_partial_content(tmpdir):
    data = "a" * 512 * 3 + "b" * 42
    with pytest.raises(errors.PartialContent) as e:
        copy_from_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_some_bytes(tmpdir):
    data = "a" * 42
    assert copy_from_image(tmpdir, data, len(data)) == data


def test_copy_to_image_some_bytes_partial_content(tmpdir):
    data = "a" * 42
    with pytest.raises(errors.PartialContent) as e:
        copy_from_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_from_aligned_image_partial(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2
    size = len(data) - 500
    assert copy_from_image(tmpdir, data, size) == data[:size]


def copy_from_image(tmpdir, data, size):
    src = tmpdir.join("src")
    src.write(data)
    dst = cStringIO.StringIO()
    directio.copy_from_image(str(src), dst, size)
    return dst.getvalue()


def test_copy_to_image_full_blocks(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2
    assert copy_to_image(tmpdir, data, len(data)) == data


def test_copy_to_image_full_blocks_partial_content(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2
    with pytest.raises(errors.PartialContent) as e:
        copy_to_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1

def test_copy_to_image_full_blocks_and_partial_block(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3
    assert copy_to_image(tmpdir, data, len(data)) == data


def test_copy_to_image_full_blocks_and_partial_block_partial_content(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3
    with pytest.raises(errors.PartialContent) as e:
        copy_to_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_full_block_and_partial_and_some(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3 + "c" * 42
    assert copy_to_image(tmpdir, data, len(data)) == data


def test_copy_to_image_full_block_and_partial_and_some_partial_content(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2 + "b" * 512 * 3 + "c" * 42
    with pytest.raises(errors.PartialContent) as e:
        copy_to_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_partial_block(tmpdir):
    data = "a" * 512 * 3
    assert copy_to_image(tmpdir, data, len(data)) == data


def test_copy_to_image_partial_block_partial_content(tmpdir):
    data = "a" * 512 * 3
    with pytest.raises(errors.PartialContent) as e:
        copy_to_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_partial_block_and_then_some(tmpdir):
    data = "a" * 512 * 3 + "b" * 42
    assert copy_to_image(tmpdir, data, len(data)) == data


def test_copy_to_image_partial_block_and_then_some_partial_content(tmpdir):
    data = "a" * 512 * 3 + "b" * 42
    with pytest.raises(errors.PartialContent) as e:
        copy_to_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_image_some_bytes(tmpdir):
    data = "a" * 42
    assert copy_to_image(tmpdir, data, len(data)) == data


def test_copy_to_image_some_bytes_partial_content(tmpdir):
    data = "a" * 42
    with pytest.raises(errors.PartialContent) as e:
        copy_to_image(tmpdir, data[:-1], len(data))
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def test_copy_to_aligned_image_partial(tmpdir):
    data = "a" * directio.BLOCKSIZE * 2
    size = len(data) - 500
    assert copy_to_image(tmpdir, data, size) == data[:size]


def copy_to_image(tmpdir, data, size):
    dst = tmpdir.join("dst")
    dst.write("")
    src = cStringIO.StringIO(data)
    directio.copy_to_image(str(dst), src, size)
    return dst.read()
