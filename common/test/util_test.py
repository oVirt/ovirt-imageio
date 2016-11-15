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
