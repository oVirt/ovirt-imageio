# ovirt-imageio
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

from ovirt_imageio import errors
from ovirt_imageio import util
from ovirt_imageio.auth import Ticket

from test import testutil

CHUNK_SIZE = 8 * 1024**2


class Operation(object):
    """
    Used to fake a ops.Operation object.
    """

    def __init__(self, offset=0, size=0):
        self.offset = offset
        self.size = size
        self.done = 0

    def run(self):
        self.done = self.size


def test_transfered_nothing():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    assert ticket.transferred() == 0


def test_transfered_inactive_empty_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.run(Operation(0, 0))
    assert ticket.transferred() == 0

    ticket.run(Operation(1000, 0))
    assert ticket.transferred() == 0


def test_transfered_inactive_ordered_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.run(Operation(0, 100))
    assert ticket.transferred() == 100

    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 300


def test_transfered_inactive_unordered_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 100

    ticket.run(Operation(0, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 300


def test_transfered_inactive_overlapping_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.run(Operation(0, 120))
    assert ticket.transferred() == 120

    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(180, 120))
    assert ticket.transferred() == 300


def test_transfered_inactive_non_continuous_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    # Run 2 non-continutes operations
    ticket.run(Operation(0, 100))
    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 200

    # Run last operation filling the hole - with some overlap.
    ticket.run(Operation(80, 120))
    assert ticket.transferred() == 300


def test_transfered_ongoing_concurrent_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    # Start 2 ongoing operations:
    # ongoing: 0-0, 100-100
    # completed:
    op1 = Operation(0, 100)
    ticket._add_operation(op1)
    assert ticket.transferred() == 0
    assert ticket.active()

    op2 = Operation(100, 100)
    ticket._add_operation(op2)
    assert ticket.transferred() == 0
    assert ticket.active()

    # Consume op1 data:
    # ongoing: 0-100, 100-100
    # completed:
    op1.run()
    ticket._remove_operation(op1)
    assert ticket.transferred() == 100
    assert ticket.active()

    # Consume op2 data:
    # ongoing: 0-100, 100-200
    # completed:
    op2.run()
    ticket._remove_operation(op2)
    assert ticket.transferred() == 200
    assert not ticket.active()


def test_transfered_ongoing_overlapping_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    # Start 2 ongoing operations.
    # ongoing: 0-0, 80-80
    # completed:
    op1 = Operation(0, 120)
    op2 = Operation(80, 120)
    ticket._add_operation(op1)
    ticket._add_operation(op2)
    assert ticket.transferred() == 0
    assert ticket.active()

    # Consume op1 data:
    # ongoing: 0-120, 80-80
    # completed:
    op1.run()
    ticket._remove_operation(op1)
    assert ticket.transferred() == 120
    assert ticket.active()

    # Consume op2 data:
    # ongoing: 0-120, 80-200
    # completed:
    op2.run()
    ticket._remove_operation(op2)
    assert ticket.transferred() == 200
    assert not ticket.active()


def test_transfered_ongoing_non_continues_ops():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    # Start 2 ongoing operations.
    # ongoing: 0-0, 200-200
    # completed:
    op1 = Operation(0, 100)
    op2 = Operation(200, 100)
    ticket._add_operation(op1)
    ticket._add_operation(op2)
    assert ticket.transferred() == 0
    assert ticket.active()

    # Consume op1 data:
    # ongoing: 0-100, 200-200
    # completed:
    op1.run()
    ticket._remove_operation(op1)
    assert ticket.transferred() == 100

    # Consume op2 data:
    # ongoing: 0-100, 200-300
    # completed:
    op2.run()
    ticket._remove_operation(op2)
    assert ticket.transferred() == 200


@pytest.mark.benchmark
def test_run_operation_benchmark():
    # Run 1000000 operations with 4 concurrent threads.
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
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
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    calls = 10000

    # Add some completed ranges - assume worst case when ranges are not
    # continues.
    for i in xrange(concurrent):
        ticket.run(Operation(i * 1000, 100))

    # Add some ongoing operations - assume worst case when ranges are not
    # continues.
    for i in xrange(concurrent):
        ticket._add_operation(Operation(i * 1000 + 200, 100))

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
    {"transfer_id": 1},
    {"filename": 1},
    {"sparse": 1},
    {"dirty": 1},
])
def test_invalid_parameter(kw):
    with pytest.raises(errors.InvalidTicketParameter):
        Ticket(testutil.create_ticket(**kw))


def test_sparse_unset():
    ticket = Ticket(testutil.create_ticket())
    assert not ticket.sparse


def test_sparse():
    ticket = Ticket(testutil.create_ticket(sparse=True))
    assert ticket.sparse


def test_dirty_unset():
    ticket = Ticket(testutil.create_ticket())
    assert not ticket.dirty


def test_dirty():
    ticket = Ticket(testutil.create_ticket(dirty=True))
    assert ticket.dirty


def test_transfer_id_unset():
    ticket = Ticket(testutil.create_ticket())
    assert ticket.transfer_id is None


def test_transfer_id():
    ticket = Ticket(testutil.create_ticket(transfer_id="123"))
    assert ticket.transfer_id == "123"


def test_repr():
    ticket = Ticket(testutil.create_ticket(
        ops=["read"], filename="tmp_file"))
    ticket_repr = repr(ticket)

    info = ticket.info()
    del info["timeout"]

    for key, value in info.items():
        pair = "%s=%r" % (key, value)
        assert pair in ticket_repr


def test_ticket_run():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    op = Operation(0, 100)
    assert ticket.transferred() == op.done
    assert op.done == 0

    ticket.run(op)

    assert ticket.transferred() == op.done
    assert op.done == 100
