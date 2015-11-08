# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from cStringIO import StringIO
from contextlib import contextmanager, closing
import os
import socket

import pytest

from imaged import _directio


def test_create():
    _directio.Buffer(4096)


@pytest.mark.parametrize("size", [None, "nan"])
def test_create_bad_size(size):
    pytest.raises(TypeError, _directio.Buffer, size)


def test_create_invalid_size():
    pytest.raises(ValueError, _directio.Buffer, 0)


def test_create_odd_size():
    pytest.raises(ValueError, _directio.Buffer, 100)


def test_create_with_align():
    _directio.Buffer(4096, align=512)


def test_create_odd_align():
    pytest.raises(ValueError, _directio.Buffer, 1024, align=128)


@pytest.mark.parametrize("align", [None, "nan"])
def test_create_bad_align(align):
    pytest.raises(TypeError, _directio.Buffer, 4096, align=align)


def test_delete():
    buf = _directio.Buffer(4096)
    del buf


def test_empty():
    buf = _directio.Buffer(512)
    assert str(buf) == ""


def test_copyfrom():
    buf = _directio.Buffer(512)
    data = "it works"
    buf.copyfrom(data)
    assert str(buf) == data


def test_copyfrom_multiple():
    buf = _directio.Buffer(512)
    buf.copyfrom("first")
    buf.copyfrom("second")
    assert str(buf) == "second"


def test_copyfrom_out_of_range():
    buf = _directio.Buffer(512)
    pytest.raises(ValueError, buf.copyfrom, "x" * 513)


def test_copyfrom_not_a_string():
    buf = _directio.Buffer(512)
    pytest.raises(TypeError, buf.copyfrom, 3.14)


@pytest.mark.parametrize("count", [42, 512, 4096])
@pytest.mark.parametrize("align", [512, 4096])
def test_readfrom(tmpdir, align, count):
    buf = _directio.Buffer(4096, align=align)
    path = make_file(tmpdir, "file", "x" * count)
    with openfd(path) as fd:
        n = buf.readfrom(fd)
    assert n == count
    assert str(buf) == "x" * n


def test_readfrom_count(tmpdir):
    buf = _directio.Buffer(1024)
    path = make_file(tmpdir, "file", "x" * 700)
    with openfd(path) as fd:
        n = buf.readfrom(fd, 512)
    assert n == 512
    assert str(buf) == "x" * 512


@pytest.mark.parametrize("count", [0, 100, 1025])
def test_readfrom_count_invalid(count):
    buf = _directio.Buffer(1024)
    pytest.raises(ValueError, buf.readfrom, -1, count)


def test_write_to_stringio(tmpdir):
    buf = _directio.Buffer(512)
    buf.copyfrom("it works")
    sio = StringIO()
    sio.write(buf)
    assert sio.getvalue() == "it works"


def test_write_to_file(tmpdir):
    buf = _directio.Buffer(512)
    buf.copyfrom("it works")
    path = str(tmpdir.join("file"))
    with open(path, "w+b") as f:
        f.write(buf)
        f.flush()
        f.seek(0)
        data = f.read()
    assert data == "it works"


def test_write_to_lowlevel_socket(tmpdir):
    buf = _directio.Buffer(512)
    buf.copyfrom("it works")
    with lowlevel_socketpair() as (rsock, wsock):
        wsock.sendall(buf)
        assert rsock.recv(512) == "it works"


def test_write_to_lowlevel_socket_file(tmpdir):
    buf = _directio.Buffer(512)
    buf.copyfrom("it works")
    with lowlevel_socketpair() as (rsock, wsock):
        with wsock.makefile("w") as wfile:
            wfile.write(buf)
        assert rsock.recv(512) == "it works"


def test_write_to_socket(tmpdir):
    buf = _directio.Buffer(512)
    buf.copyfrom("it works")
    with wrapped_socketpair() as (rsock, wsock):
        wsock.sendall(buf)
        assert rsock.recv(512) == "it works"


def test_write_to_socket_file(tmpdir):
    buf = _directio.Buffer(512)
    buf.copyfrom("it works")
    with wrapped_socketpair() as (rsock, wsock):
        wfile = wsock.makefile("w")
        with closing(wfile):
            wfile.write(buf)
        assert rsock.recv(512) == "it works"


def make_file(tmpdir, name, data=""):
    f = tmpdir.join('file')
    f.write(data)
    return str(f)


@contextmanager
def openfd(path, flags=os.O_RDONLY):
    fd = os.open(path, flags | os.O_DIRECT)
    try:
        yield fd
    finally:
        os.close(fd)


@contextmanager
def lowlevel_socketpair():
    rsock, wsock = socket.socketpair()
    with closing(rsock), closing(wsock):
        yield rsock, wsock


@contextmanager
def wrapped_socketpair():
    pair = socket.socketpair()
    rsock = socket.socket(_sock=pair[0])
    wsock = socket.socket(_sock=pair[1])
    with closing(rsock), closing(wsock):
        yield rsock, wsock
