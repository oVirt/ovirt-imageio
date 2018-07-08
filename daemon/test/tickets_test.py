# ovirt-imageio-daemon
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import time

import pytest

from six.moves import xrange

from ovirt_imageio_common import errors
from ovirt_imageio_common import util
from ovirt_imageio_daemon.tickets import Ticket

from test import testutils

CHUNK_SIZE = 8 * 1024**2


class Operation(object):
    """
    Used to fake a directio.Operation object.
    """

    def __init__(self, offset=0, size=0):
        self.offset = offset
        self.size = size
        self.done = 0
        self.closed = False

    def __iter__(self):
        self.done = self.size
        yield b"x" * self.size

    def run(self):
        self.done = self.size

    def close(self):
        self.closed = True


def test_transfered_nothing():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    assert ticket.transferred() == 0


def test_transfered_inactive_empty_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    ticket.run(Operation(0, 0))
    assert ticket.transferred() == 0

    ticket.run(Operation(1000, 0))
    assert ticket.transferred() == 0


def test_transfered_inactive_ordered_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    ticket.run(Operation(0, 100))
    assert ticket.transferred() == 100

    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 300


def test_transfered_inactive_unordered_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 100

    ticket.run(Operation(0, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 300


def test_transfered_inactive_overlapping_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    ticket.run(Operation(0, 120))
    assert ticket.transferred() == 120

    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(180, 120))
    assert ticket.transferred() == 300


def test_transfered_inactive_non_continuous_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    # Run 2 non-continutes operations
    ticket.run(Operation(0, 100))
    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 200

    # Run last operation filling the hole - with some overlap.
    ticket.run(Operation(80, 120))
    assert ticket.transferred() == 300


def test_transfered_ongoing_concurrent_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))

    # Start 2 ongoing operations:
    # ongoing: 0-0, 100-100
    # completed:
    b1 = ticket.bind(Operation(0, 100))
    b2 = ticket.bind(Operation(100, 100))
    assert ticket.transferred() == 0
    assert ticket.active()

    # Consume b1 data:
    # ongoing: 0-100, 100-100
    # completed:
    list(b1)
    assert ticket.transferred() == 100
    assert ticket.active()

    # Consume b2 data:
    # ongoing: 0-100, 100-200
    # completed:
    list(b2)
    assert ticket.transferred() == 200
    assert ticket.active()

    # Close first operation:
    # ongoing: 100-200
    # completed: 0-100
    b1.close()
    assert ticket.transferred() == 200
    assert ticket.active()

    # Close last operation:
    # ongoing:
    # completed: 0-200
    b2.close()
    assert ticket.transferred() == 200
    assert not ticket.active()


def test_transfered_ongoing_overlapping_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))

    # Start 2 ongoing operations.
    # ongoing: 0-0, 80-80
    # completed:
    b1 = ticket.bind(Operation(0, 120))
    b2 = ticket.bind(Operation(80, 120))
    assert ticket.transferred() == 0
    assert ticket.active()

    # Consume b1 data:
    # ongoing: 0-120, 80-80
    # completed:
    list(b1)
    assert ticket.transferred() == 120

    # Consume b2 data:
    # ongoing: 0-120, 80-200
    # completed:
    list(b2)
    assert ticket.transferred() == 200

    # Close first operation:
    # ongoing: 80-200
    # completed: 0-120
    b1.close()
    assert ticket.transferred() == 200

    # Close last operation:
    # ongoing:
    # completed: 0-200
    b2.close()
    assert ticket.transferred() == 200


def test_transfered_ongoing_non_continues_ops():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))

    # Start 2 ongoing operations.
    # ongoing: 0-0, 200-200
    # completed:
    b1 = ticket.bind(Operation(0, 100))
    b2 = ticket.bind(Operation(200, 100))
    assert ticket.transferred() == 0
    assert ticket.active()

    # Consume b1 data:
    # ongoing: 0-100, 200-200
    # completed:
    list(b1)
    assert ticket.transferred() == 100

    # Consume b2 data:
    # ongoing: 0-100, 200-300
    # completed:
    list(b2)
    assert ticket.transferred() == 200

    # Close first operation:
    # ongoing: 200-300
    # completed: 0-100
    b1.close()
    assert ticket.transferred() == 200

    # Close last operation:
    # ongoing:
    # completed: 0-100, 200-300
    b2.close()
    assert ticket.transferred() == 200


@pytest.mark.benchmark
def test_run_operation_benchmark():
    # Run 1000000 operations with 4 concurrent threads.
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    operations = 10**6
    workers = 4
    chunk = 10**9
    step = chunk * workers // operations

    def worker(offset, size):
        while offset < size:
            ticket.run(Operation(offset, step))
            offset += step

    start = time.time()

    threads = []
    try:
        for i in range(workers):
            t = util.start_thread(worker, args=(i * chunk, chunk))
            threads.append(t)
    finally:
        for t in threads:
            t.join()

    elapsed = time.time() - start

    print("%d operations, %d concurrent threads in %.2f seconds (%d nsec/op)"
          % (operations, workers, elapsed, elapsed * 10**9 // operations))


@pytest.mark.benchmark
@pytest.mark.parametrize("concurrent", [1, 2, 4, 8])
def test_transferred_benchmark(concurrent):
    # Time trransferred call with multiple ongoing and completed operations.
    ticket = Ticket(testutils.create_ticket(ops=["read"]))

    calls = 10000

    # Add some completed ranges - assume worst case when ranges are not
    # continues.
    for i in xrange(concurrent):
        ticket.run(Operation(i * 1000, 100))

    # Add some ongoing operations - assume worst case when ranges are not
    # continues.
    for i in xrange(concurrent):
        list(ticket.bind(Operation(i * 1000 + 200, 100)))

    # Time transferred call - merging ongoing and completed ranges.
    start = time.time()
    for i in xrange(calls):
        ticket.transferred()
    elapsed = time.time() - start

    print("%d concurrent operations, %d calls in %.2f seconds (%d nsec/op)"
          % (concurrent, calls, elapsed, elapsed * 10**9 // calls))


@pytest.mark.parametrize("arg", [
    "not a dict",
    ["not", "a", "dict"],
    1,
    3.1,
    True,
    False,
    None
])
def test_invalid_argument(arg):
    with pytest.raises(errors.InvalidTicket):
        Ticket(arg)


@pytest.mark.parametrize("kw", [
    {"uuid": 1},
    {"size": "not an int"},
    {"ops": "not a list"},
    {"timeout": "not an int"},
    {"url": 1},
    {"filename": 1},
    {"sparse": 1},
])
def test_invalid_parameter(kw):
    with pytest.raises(errors.InvalidTicketParameter):
        Ticket(testutils.create_ticket(**kw))


def test_sparse_unset():
    ticket = Ticket(testutils.create_ticket())
    assert not ticket.sparse


def test_sparse():
    ticket = Ticket(testutils.create_ticket(sparse=True))
    assert ticket.sparse


def test_repr():
    ticket = Ticket(testutils.create_ticket(
        ops=["read"], filename="tmp_file"))
    ticket_repr = repr(ticket)

    info = ticket.info()
    del info["timeout"]

    for key, value in info.items():
        pair = "%s=%r" % (key, value)
        assert pair in ticket_repr


def test_ticket_run():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    op = Operation(0, 100)
    assert ticket.transferred() == op.done
    assert op.done == 0

    ticket.run(op)

    assert ticket.transferred() == op.done
    assert op.done == 100
    assert not op.closed


def test_ticket_bind():
    ticket = Ticket(testutils.create_ticket(ops=["read"]))
    op = Operation(0, 100)
    bop = ticket.bind(op)

    assert ticket.active()
    assert ticket.transferred() == 0
    assert op.done == 0

    # Use as WebOB.Response.app_iter.
    data = list(bop)

    assert op.done == 100
    assert not op.closed
    assert data == [b"x" * op.done]

    bop.close()

    assert not ticket.active()
    assert ticket.transferred() == op.done
    assert op.closed
