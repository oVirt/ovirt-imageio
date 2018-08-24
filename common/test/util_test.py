# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import os
import signal
import sys
import time

import pytest

from ovirt_imageio_common import util

pytestmark = pytest.mark.skipif(sys.version_info[0] > 2,
                                reason='needs porting to python 3')


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
            os.write(w, 'a')

        util.start_thread(write)
        assert util.uninterruptible(read) == 'a'
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
        "[total=3.000000, read=1.000000, write=1.000000, sync=1.000000]")


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
        "[total=5.000000, read=2.000000, write=2.000000, sync=1.000000]")


def test_clock_running(fake_time):
    c = util.Clock()
    c.start("total")
    fake_time.value += 3
    c.start("read")
    fake_time.value += 4
    c.stop("read")
    assert str(c) == "[total=7.000000*, read=4.000000]"


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
    assert str(c) == "[total=7.000000, a=4.000000, b=3.000000]"


def test_clock_run_recursive():
    c = util.Clock()
    with c.run("started"):
        with pytest.raises(RuntimeError):
            with c.run("started"):
                pass
