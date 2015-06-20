# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from contextlib import contextmanager, closing
import socket
import threading

from imaged import directio


def test_copy_from_image(tmpdir):
    data = "a" * directio.BLOCKSIZE * 10
    assert copy_from_image(tmpdir, data) == data


def test_copy_from_image_extra(tmpdir):
    data = "a" * directio.BLOCKSIZE + "b" * 42
    assert copy_from_image(tmpdir, data) == data


def test_copy_from_image_partial(tmpdir):
    data = "a" * 42
    assert copy_from_image(tmpdir, data) == data


def copy_from_image(tmpdir, data):
    src = tmpdir.join("src")
    src.write(data)
    received = [""]
    with sockfiles() as (rfile, wfile):
        def reader():
            received[0] = rfile.read(len(data))
        t = threading.Thread(target=reader)
        t.daemon = True
        t.start()
        directio.copy_from_image(str(src), wfile, len(data))
        t.join()
    return received[0]


def test_copy_to_image(tmpdir):
    data = "a" * directio.BLOCKSIZE * 10
    assert copy_to_image(tmpdir, data) == data


def test_copy_to_image_extra(tmpdir):
    data = "a" * directio.BLOCKSIZE + "b" * 42
    assert copy_to_image(tmpdir, data) == data


def test_copy_to_image_partial(tmpdir):
    data = "a" * 42
    assert copy_to_image(tmpdir, data) == data


def copy_to_image(tmpdir, data):
    dst = tmpdir.join("dst")
    with open(str(dst), "w") as f:
        f.truncate(len(data))
    with sockfiles() as (rfile, wfile):
        def writer():
            wfile.write(data)
            wfile.flush()
        t = threading.Thread(target=writer)
        t.daemon = True
        t.start()
        directio.copy_to_image(str(dst), rfile, len(data))
        t.join()
    return dst.read()


@contextmanager
def sockfiles():
    # socketpair returns raw platform sockets, which cannot be wrapped by
    # ssl.wrap_socket. It must be wrapped in socket.socket() to use ssl.
    pair = socket.socketpair()
    sock1 = socket.socket(_sock=pair[0])
    sock2 = socket.socket(_sock=pair[1])
    with closing(sock1), closing(sock2):
        rfile = sock1.makefile("rb")
        wfile = sock2.makefile("wb")
        with closing(rfile), closing(wfile):
            yield rfile, wfile
