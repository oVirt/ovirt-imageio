# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest
from ovirt_imageio._internal import stats


class FakeTime:

    def __init__(self):
        self.value = 0

    def __call__(self):
        return self.value


# Ccorrect usage

def test_empty():
    c = stats.Clock()
    assert str(c) == ""


def test_stop_returns_elapsed_time():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)

    c.start("read")
    fake_time.value += 1
    assert c.stop("read") == 1

    c.start("read")
    fake_time.value += 2
    assert c.stop("read") == 2


def test_measure():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)
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
        "[total 1 ops, 3.000000 s] "
        "[read 1 ops, 1.000000 s] "
        "[write 1 ops, 1.000000 s] "
        "[sync 1 ops, 1.000000 s]"
    )


def test_measure_multiple():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)
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
        "[total 1 ops, 5.000000 s] "
        "[read 2 ops, 2.000000 s] "
        "[write 2 ops, 2.000000 s] "
        "[sync 1 ops, 1.000000 s]"
    )


def test_running():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)
    c.start("total")
    fake_time.value += 3
    c.start("read")
    fake_time.value += 4
    c.stop("read")
    # Since total was not stopped, the operation is not counted.
    assert str(c) == "[total 0 ops, 7.000000 s] [read 1 ops, 4.000000 s]"


def test_bytes():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)

    c.start("total")

    # Using start()/stop().

    s = c.start("read")
    s.bytes += 512 * 1024**2
    fake_time.value += 1
    c.stop("read")

    s = c.start("write")
    s.bytes += 200 * 1024**2
    fake_time.value += 1
    c.stop("write")

    # Using run().

    with c.run("read") as s:
        s.bytes += 512 * 1024**2
        fake_time.value += 1

    with c.run("write") as s:
        s.bytes += 200 * 1024**2
        fake_time.value += 1

    with c.run("sync"):
        fake_time.value += 1

    c.stop("total")

    assert str(c) == (
        "[total 1 ops, 5.000000 s] "
        "[read 2 ops, 2.000000 s, 1.00 GiB, 512.00 MiB/s] "
        "[write 2 ops, 2.000000 s, 400.00 MiB, 200.00 MiB/s] "
        "[sync 1 ops, 1.000000 s]"
    )


def test_null_clock():
    c = stats.NullClock()

    # This clock records nothing...
    with c.run("total"):
        with c.run("read") as s:
            s.bytes += 4096
            # bytes value is dropped...
            assert s.bytes == 0
        with c.run("sync"):
            pass

    # And always return empty string.
    assert str(c) == ""


def test_error_before_stop():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)
    c.start("read")
    fake_time.value += 1

    # User code fails here, before we increase bytes and call stop().

    # Since read was not stopped the operation is not counted...
    assert str(c) == "[read 0 ops, 1.000000 s]"

    # And we cannot start this operation again.
    with pytest.raises(RuntimeError):
        c.start("read")

    # We can abort the operation to continue to use this clock.
    fake_time.value += 1
    c.abort("read")
    assert str(c) == "[read 0 ops, 2.000000 s]"


def test_error_in_run():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)

    with pytest.raises(RuntimeError):
        with c.run("read") as s:
            fake_time.value += 1
            raise RuntimeError("fake error")

            s.bytes += 512 * 1024**2

    # First read failed, so bytes is not set and the operation is not counted.
    assert str(c) == "[read 0 ops, 1.000000 s]"

    # However clock was stopped, so next read can succeed.
    with c.run("read") as s:
        fake_time.value += 1
        s.bytes += 512 * 1024**2

    # Bytes value is set now so we report total and rate values.
    assert str(c) == "[read 1 ops, 2.000000 s, 512.00 MiB, 256.00 MiB/s]"


# Inccorrect usage

def test_start_twice():
    c = stats.Clock()
    c.start("started")
    with pytest.raises(RuntimeError):
        c.start("started")


def test_stop_twice():
    c = stats.Clock()
    c.start("stopped")
    c.stop("stopped")
    with pytest.raises(RuntimeError):
        c.stop("stopped")


def test_stop_missing():
    c = stats.Clock()
    with pytest.raises(RuntimeError):
        c.stop("missing")


def test_run():
    fake_time = FakeTime()
    c = stats.Clock(fake_time)
    with c.run("total"):
        with c.run("a"):
            fake_time.value += 4
        with c.run("b"):
            fake_time.value += 3
    assert str(c) == (
        "[total 1 ops, 7.000000 s] "
        "[a 1 ops, 4.000000 s] "
        "[b 1 ops, 3.000000 s]"
    )


def test_run_recursive():
    c = stats.Clock()
    with c.run("started"):
        with pytest.raises(RuntimeError):
            with c.run("started"):
                pass


@pytest.mark.benchmark
@pytest.mark.parametrize("clock", [
    pytest.param(stats.Clock(), id="clock"),
    pytest.param(stats.NullClock(), id="null-clock"),
])
def test_benchmark(clock):
    test = stats.Clock()
    with test.run("total"):
        clock.start("connection")
        # We have seen 66,000 requests per single upload with virt-v2v.
        for i in range(50000):
            clock.start("request")
            with clock.run("read") as s:
                s.bytes += 2 * 1024**2
            with clock.run("write") as s:
                s.bytes += 2 * 1024**2
            clock.stop("request")
        clock.stop("connection")
    print("{} {}".format(test, clock))
