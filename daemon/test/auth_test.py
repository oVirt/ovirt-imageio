# ovirt-imageio
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import time

import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import errors
from ovirt_imageio._internal import ops
from ovirt_imageio._internal import util
from ovirt_imageio._internal.auth import Ticket, Authorizer

from test import testutil

CHUNK_SIZE = 8 * 1024**2


class Operation:
    """
    Used to fake a ops.Operation object.
    """

    def __init__(self, offset=0, size=0):
        self.offset = offset
        self.size = size
        self.done = 0
        self.canceled = False

    def run(self):
        if self.canceled:
            raise ops.Canceled
        self.done = self.size

    def cancel(self):
        self.canceled = True


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

    start = time.monotonic()

    threads = []
    try:
        for i in range(workers):
            t = util.start_thread(worker, args=(i * chunk, chunk))
            threads.append(t)
    finally:
        for t in threads:
            t.join()

    elapsed = time.monotonic() - start

    print("%d operations, %d concurrent threads in %.3f seconds (%d nsec/op)"
          % (operations, workers, elapsed, elapsed * 10**9 // operations))


@pytest.mark.benchmark
@pytest.mark.parametrize("concurrent", [1, 2, 4, 8])
def test_transferred_benchmark(concurrent):
    # Time trransferred call with multiple ongoing and completed operations.
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    calls = 10000

    # Add some completed ranges - assume worst case when ranges are not
    # continues.
    for i in range(concurrent):
        ticket.run(Operation(i * 1000, 100))

    # Add some ongoing operations - assume worst case when ranges are not
    # continues.
    for i in range(concurrent):
        ticket._add_operation(Operation(i * 1000 + 200, 100))

    # Time transferred call - merging ongoing and completed ranges.
    start = time.monotonic()
    for i in range(calls):
        ticket.transferred()
    elapsed = time.monotonic() - start

    print("%d concurrent operations, %d calls in %.3f seconds (%d nsec/op)"
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


def test_cancel_unused():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.cancel()

    # Ticket is canceled and can be removed immediately.
    assert ticket.canceled
    info = ticket.info()
    assert info["canceled"]
    assert info["connections"] == 0


def test_cancel_timeout():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.add_context(1, None)

    # Canceling will time out.
    with pytest.raises(errors.TicketCancelTimeout):
        ticket.cancel(timeout=0.001)

    # But ticket is marked as canceled.
    assert ticket.canceled

    # Caller can poll ticket status and remove the ticket when number of
    # connections is zero.
    info = ticket.info()
    assert info["canceled"]
    assert info["connections"] == 1


def test_cancel_async():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.add_context(1, None)
    ticket.cancel(timeout=0)

    # Ticket is canceled, but it cannot be removed.
    assert ticket.canceled
    info = ticket.info()
    assert info["canceled"]
    assert info["connections"] == 1


def test_cancel_ongoing_operations():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    # Few connections are using this ticket. Each running an operation.
    ops = []
    for i in range(4):
        ctx = Context()
        op = Operation()
        ticket.add_context(i, ctx)
        ticket._add_operation(op)
        ops.append(op)

    ticket.cancel(timeout=0)

    # All ongoing operations are canceled.
    assert all(op.canceled for op in ops)


class Context:

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_cancel_wait():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    # Add connections using this ticket.
    connections = []
    for i in range(4):
        ctx = Context()
        ticket.add_context(i, ctx)
        connections.append(ctx)

    def close_connections():
        time.sleep(0.1)
        for i in range(4):
            ticket.remove_context(i)

    info = ticket.info()
    assert not info["canceled"]
    assert info["connections"] == 4

    t = util.start_thread(close_connections)
    try:
        ticket.cancel(timeout=10)

        # After the ticket was canceled, number of connections must be zero.
        info = ticket.info()
        assert info["connections"] == 0

        # And all contexts must be closed.
        assert all(ctx.closed for ctx in connections)
    finally:
        t.join()


def test_canceled_fail_run_before():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.cancel()

    op = Operation()

    # Running operations must fail.
    with pytest.raises(errors.AuthorizationError):
        ticket.run(op)

    # Operation was not run.
    assert op.done == 0


def test_canceled_fail_run_after():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))

    class Operation:

        def __init__(self):
            self.done = False
            self.canceled = False

        def run(self):
            self.done = True
            ticket.cancel()

        def cancel(self):
            self.canceled = True

    op = Operation()

    # If ticket was canceled while ongoing operations are running, ticket run
    # will fail removing the operations.
    with pytest.raises(errors.AuthorizationError):
        ticket.run(op)

    assert op.done


def test_canceled_fail_add_context():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.cancel()

    ctx = Context()

    # Adding new context must fail.
    with pytest.raises(errors.AuthorizationError):
        ticket.add_context(2, ctx)


def test_get_context_missing():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    with pytest.raises(KeyError):
        ticket.get_context(1)


def test_get_context():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ctx = Context()
    ticket.add_context(1, ctx)
    assert ticket.get_context(1) is ctx


def test_remove_context_missing():
    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.add_context(1, Context())
    assert ticket.info()["connections"] == 1

    ticket.remove_context(2)
    assert ticket.info()["connections"] == 1


def test_remove_context_error():

    class FailingContext:

        def __init__(self):
            self.count = 1
            self.closed = False

        def close(self):
            if self.count > 0:
                self.count -= 1
                raise RuntimeError("Cannot close yet")
            self.closed = True

    ticket = Ticket(testutil.create_ticket(ops=["read"]))
    ticket.add_context(1, FailingContext())

    ticket.cancel(timeout=0)

    # If closing a context fails, keep it. The ticket cannot be removed until
    # this context is removed successfully.
    with pytest.raises(RuntimeError):
        ticket.remove_context(1)

    info = ticket.info()
    assert info["connections"] == 1

    # Calling again will close and remove the context successfully.
    ticket.remove_context(1)

    info = ticket.info()
    assert info["connections"] == 0


def test_authorizer_add():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    assert ticket.uuid == ticket_info["uuid"]


def test_authorizer_remove_unused():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    # Ticket is unused so it will be removed.
    auth.remove(ticket_info["uuid"])
    with pytest.raises(KeyError):
        auth.get(ticket_info["uuid"])


def test_authorizer_remove_timeout():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    ticket.add_context(1, Context())
    assert ticket.info()["connections"] == 1

    # Use short timeout to keep the tests fast.
    cfg.control.remove_timeout = 0.001

    # Ticket cannot be removed since it is used by connection 1.
    with pytest.raises(errors.TicketCancelTimeout):
        auth.remove(ticket.uuid)

    # Ticket was not removed.
    assert auth.get(ticket.uuid) is ticket

    # The connection was closed, the ticket can be removed now.
    ticket.remove_context(1)
    assert ticket.info()["connections"] == 0

    auth.remove(ticket.uuid)

    # Ticket was removed.
    with pytest.raises(KeyError):
        auth.get(ticket.uuid)


def test_authorizer_remove_async():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    ticket.add_context(1, Context())
    assert ticket.info()["connections"] == 1

    # Disable the timeout, so removing a ticket cancel the ticket without
    # waiting, and requiring polling the ticket status.
    cfg.control.remove_timeout = 0

    # Ticket is canceled, but not removed.
    auth.remove(ticket.uuid)
    assert ticket.canceled
    assert ticket.info()["connections"] == 1

    # Ticket was not removed.
    assert auth.get(ticket.uuid) is ticket

    # The connection was closed, the ticket can be removed now.
    ticket.remove_context(1)
    assert ticket.info()["connections"] == 0

    auth.remove(ticket.uuid)

    # Ticket was removed.
    with pytest.raises(KeyError):
        auth.get(ticket.uuid)


def test_authorizer_remove_mising():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    # Removing missing ticket does not raise.
    auth.remove("no-such-ticket")


def test_authorize_read():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    assert auth.authorize(ticket.uuid, "read") == ticket

    with pytest.raises(errors.AuthorizationError):
        auth.authorize(ticket.uuid, "write")


def test_authorize_write():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["write"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    assert auth.authorize(ticket.uuid, "write") == ticket

    # "write" implies also "read".
    assert auth.authorize(ticket.uuid, "read") == ticket


def test_authorizer_no_ticket():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    with pytest.raises(errors.AuthorizationError):
        auth.authorize("no-such-ticket", "read")


@pytest.mark.parametrize("ops,allowed", [
    (["read"], ["read"]),
    (["write"], ["read", "write"]),
])
def test_authorizer_canceled(ops, allowed):
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=ops)
    auth.add(ticket_info)
    ticket = auth.get(ticket_info["uuid"])

    # Cancelling the ticket disables any operation.
    ticket.cancel()

    for op in allowed:
        with pytest.raises(errors.AuthorizationError):
            auth.authorize(ticket.uuid, op)


def test_authorizer_expired():
    cfg = config.load(["test/conf.d/daemon.conf"])
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["write"])
    auth.add(ticket_info)
    ticket = auth.get(ticket_info["uuid"])

    # Extending with zero timeout expire the ticket.
    ticket.extend(0)

    for op in ("read", "write"):
        with pytest.raises(errors.AuthorizationError):
            auth.authorize(ticket.uuid, op)
