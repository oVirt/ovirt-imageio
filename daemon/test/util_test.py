# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import
from __future__ import print_function

import os
import signal
import time

import pytest
import six

from ovirt_imageio import util


def test_uninterruptible_interrupt():
    r, w = os.pipe()
    signo = signal.SIGUSR1
    prev = signal.signal(signo, lambda s, f: True)
    try:

        def read():
            return os.read(r, 1)

        def write():
            time.sleep(0.1)
            os.kill(os.getpid(), signo)
            time.sleep(0.1)
            os.write(w, b'a')

        util.start_thread(write)
        assert util.uninterruptible(read) == b'a'
    finally:
        signal.signal(signo, prev)
        os.close(r)
        os.close(w)


def test_uninterruptible_raise():
    def fail():
        raise OSError(0, "fake")
    pytest.raises(OSError, util.uninterruptible, fail)


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


class FakeTime(object):

    def __init__(self):
        self.value = 0

    def __call__(self):
        return self.value


@pytest.fixture
def fake_time(monkeypatch):
    t = FakeTime()
    monkeypatch.setattr(time, "time", t)
    yield t


# Ccorrect usage

def test_clock_empty():
    c = util.Clock()
    assert str(c) == "[]"


def test_clock_stop_returns_elapsed_time(fake_time):
    c = util.Clock()

    c.start("read")
    fake_time.value += 1
    assert c.stop("read") == 1

    c.start("read")
    fake_time.value += 2
    assert c.stop("read") == 2


def test_clock_measure(fake_time):
    c = util.Clock()
    c.start("total")
    c.start("read")
    fake_time.value += 1
    c.stop("read")
    c.start("write")
    fake_time.value += 1
    c.stop("write")
    c.start("sync")
    fake_time.value += 1
    c.stop("sync")
    c.stop("total")
    assert str(c) == (
        "[total=3.000000/1, read=1.000000/1, write=1.000000/1, "
        "sync=1.000000/1]")


def test_clock_measure_multiple(fake_time):
    c = util.Clock()
    c.start("total")
    c.start("read")
    fake_time.value += 1
    c.stop("read")
    c.start("write")
    fake_time.value += 1
    c.stop("write")
    c.start("read")
    fake_time.value += 1
    c.stop("read")
    c.start("write")
    fake_time.value += 1
    c.stop("write")
    c.start("sync")
    fake_time.value += 1
    c.stop("sync")
    c.stop("total")
    assert str(c) == (
        "[total=5.000000/1, read=2.000000/2, write=2.000000/2, "
        "sync=1.000000/1]")


def test_clock_running(fake_time):
    c = util.Clock()
    c.start("total")
    fake_time.value += 3
    c.start("read")
    fake_time.value += 4
    c.stop("read")
    assert str(c) == "[total=7.000000/1, read=4.000000/1]"


# Inccorrect usage

def test_clock_start_twice():
    c = util.Clock()
    c.start("started")
    with pytest.raises(RuntimeError):
        c.start("started")


def test_clock_stop_twice():
    c = util.Clock()
    c.start("stopped")
    c.stop("stopped")
    with pytest.raises(RuntimeError):
        c.stop("stopped")


def test_clock_stop_missing():
    c = util.Clock()
    with pytest.raises(RuntimeError):
        c.stop("missing")


def test_clock_run(fake_time):
    c = util.Clock()
    with c.run("total"):
        with c.run("a"):
            fake_time.value += 4
        with c.run("b"):
            fake_time.value += 3
    assert str(c) == "[total=7.000000/1, a=4.000000/1, b=3.000000/1]"


def test_clock_run_recursive():
    c = util.Clock()
    with c.run("started"):
        with pytest.raises(RuntimeError):
            with c.run("started"):
                pass


@pytest.mark.benchmark
def test_benchmark():
    c = util.Clock()
    c.start("connection")
    # We have seen 66,000 requests per single upload with virt-v2v.
    for i in range(50000):
        c.start("request")
        c.start("read")
        c.stop("read")
        c.start("write")
        c.stop("write")
        c.stop("request")
    c.stop("connection")
    print(c)


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
    assert isinstance(result, six.text_type)
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
