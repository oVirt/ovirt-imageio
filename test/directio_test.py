# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import cStringIO
import pytest
import string
from imaged import directio
from imaged import errors


def fill(s, size):
    count, rest = divmod(size, len(s))
    return s * count + s[:rest]


BUFFER = fill(string.uppercase, directio.BUFFERSIZE)
PARTIAL = fill(string.lowercase, directio.BLOCKSIZE)
BYTES = fill(string.digits, 42)


class param(str):
    """ Prevent pytest from showing the value in the test name """
    def __str__(self):
        return self[:10]


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    param(BUFFER * 2),
    param(BUFFER + PARTIAL * 2),
    param(BUFFER + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
])
def test_send(tmpdir, data, offset):
    size = len(data) - offset
    expected = data[offset:]
    assert send(tmpdir, data, size, offset=offset) == expected


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize(
    "size", [511, 513, len(BUFFER) + 511, len(BUFFER) + 513])
def test_send_partial(tmpdir, size, offset):
    data = BUFFER * 2
    expected = data[offset:offset + size]
    assert send(tmpdir, data, size, offset=offset) == expected


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    param(BUFFER * 2),
    param(BUFFER + PARTIAL * 2),
    param(BUFFER + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
])
def test_send_partial_content(tmpdir, data, offset):
    size = len(data) - offset
    with pytest.raises(errors.PartialContent) as e:
        send(tmpdir, data[:-1], size, offset=offset)
    assert e.value.requested == size
    assert e.value.available == size - 1


def send(tmpdir, data, size, offset=0):
    src = tmpdir.join("src")
    src.write(data)
    dst = cStringIO.StringIO()
    op = directio.Send(str(src), dst, size, offset=offset)
    op.run()
    return dst.getvalue()


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    param(BUFFER * 2),
    param(BUFFER + PARTIAL * 2),
    param(BUFFER + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
    param(BYTES),
])
def test_receive(tmpdir, data, offset):
    assert receive(tmpdir, data, len(data), offset=offset) == data


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize(
    "size", [511, 513, len(BUFFER) + 511, len(BUFFER) + 513])
def test_receive_partial(tmpdir, size, offset):
    data = BUFFER * 2
    assert receive(tmpdir, data, size, offset=offset) == data[:size]


@pytest.mark.parametrize("offset", [0, 42, 512])
@pytest.mark.parametrize("data", [
    param(BUFFER * 2),
    param(BUFFER + PARTIAL * 2),
    param(BUFFER + PARTIAL + BYTES),
    param(PARTIAL * 2),
    param(PARTIAL + BYTES),
    param(BYTES),
])
def test_receive_partial_content(tmpdir, data, offset):
    with pytest.raises(errors.PartialContent) as e:
        receive(tmpdir, data[:-1], len(data), offset=offset)
    assert e.value.requested == len(data)
    assert e.value.available == len(data) - 1


def receive(tmpdir, data, size, offset=0):
    dst = tmpdir.join("dst")
    dst.write("x" * offset)
    src = cStringIO.StringIO(data)
    op = directio.Receive(str(dst), src, size, offset=offset)
    op.run()
    with open(str(dst), "rb") as f:
        f.seek(offset)
        return f.read()


def test_send_repr():
    op = directio.Send("/path", None, 200, offset=24)
    rep = repr(op)
    assert "Send" in rep
    assert "path='/path' size=200 offset=24 buffersize=512 done=0" in rep


def test_recv_repr():
    op = directio.Receive("/path", None, 100, offset=42)
    rep = repr(op)
    assert "Receive" in rep
    assert "path='/path' size=100 offset=42 buffersize=512 done=0" in rep


@pytest.mark.parametrize("size,rounded", [
    (0, 0),
    (1, 512),
    (512, 512),
    (513, 1024),
])
def test_round_up(size, rounded):
    assert directio.round_up(size) == rounded
