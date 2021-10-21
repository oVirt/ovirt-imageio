# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import time

import pytest

from ovirt_imageio._internal import util


def test_start_thread_args():
    result = []

    def f(*a):
        result.extend(a)

    util.start_thread(f, args=(1, 2)).join()
    assert result == [1, 2]


def test_start_thread_kwargs():
    result = {}

    def f(k=None):
        result["k"] = k

    util.start_thread(f, kwargs={"k": "v"}).join()
    assert result == {"k": "v"}


def test_start_thread_name():
    t = util.start_thread(lambda: None, name="foo")
    t.join()
    assert t.name == "foo"


def test_start_thread_daemon():
    t = util.start_thread(lambda: None)
    t.join()
    assert t.daemon


def test_start_thread_non_daemon():
    t = util.start_thread(lambda: None, daemon=False)
    t.join()
    assert not t.daemon


def test_monotonic_time():
    t1 = util.monotonic_time()
    time.sleep(0.01)
    t2 = util.monotonic_time()
    assert t1 <= t2


@pytest.mark.parametrize("size,rounded", [
    (0, 0),
    (1, 512),
    (512, 512),
    (512 + 1, 512 * 2),
])
def test_round_up(size, rounded):
    assert util.round_up(size, 512) == rounded


@pytest.mark.parametrize("size,rounded", [
    (0, 0),
    (1, 0),
    (512, 512),
    (512 + 1, 512),
])
def test_round_down(size, rounded):
    assert util.round_down(size, 512) == rounded


@pytest.mark.parametrize("value,expected", [
    ("value", "value"),
    ("value", "value"),
    (b"value", "value"),
    ("\u05d0", "\u05d0"),
    (b"\xd7\x90", "\u05d0"),
    ("\u0000", "\u0000"),
    (b"\0", "\u0000"),
])
def test_ensure_text(value, expected):
    result = util.ensure_text(value)
    assert isinstance(result, str)
    assert result == expected


def test_ensure_text_unexpected_type():
    with pytest.raises(TypeError):
        util.ensure_text(1)


def test_unbuffered_stream_more():
    chunks = [b"1" * 256,
              b"2" * 256,
              b"3" * 42,
              b"4" * 256]
    s = util.UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(512)
    assert b == chunks[0]
    # Chunk 2
    b = s.read(512)
    assert b == chunks[1]
    # Chunk 3
    b = s.read(512)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(512)
    assert b == chunks[3]
    # Empty
    b = s.read(512)
    assert b == b''
    b = s.read(512)
    assert b == b''


def test_unbuffered_stream_less():
    chunks = [b"1" * 256,
              b"2" * 256,
              b"3" * 42,
              b"4" * 256]
    s = util.UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(128)
    assert b == chunks[0][:128]
    b = s.read(128)
    assert b == chunks[0][128:]
    # Chunk 2
    b = s.read(128)
    assert b == chunks[1][:128]
    b = s.read(128)
    assert b == chunks[1][128:]
    # Chunk 3
    b = s.read(128)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(128)
    assert b == chunks[3][:128]
    b = s.read(128)
    assert b == chunks[3][128:]
    # Empty
    b = s.read(128)
    assert b == b''
    b = s.read(128)
    assert b == b''


@pytest.mark.parametrize("n,s", [
    (0, "0 bytes"),
    (0.0, "0 bytes"),
    (1023, "1023 bytes"),
    (1024, "1.00 KiB"),
    (1024 * 1023, "1023.00 KiB"),
    (1024 * 1024, "1.00 MiB"),
    (1024**2 * 1023, "1023.00 MiB"),
    (1024**2 * 1024, "1.00 GiB"),
    (1024**3 * 1023, "1023.00 GiB"),
    (1024**3 * 1024, "1.00 TiB"),
    (1024**4 * 1023, "1023.00 TiB"),
    (1024**4 * 1024, "1.00 PiB"),
])
def test_humansize(n, s):
    assert util.humansize(n) == s
