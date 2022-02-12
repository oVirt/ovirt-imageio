# ovirt-imageio
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import itertools
import logging
import time

import pytest

from ovirt_imageio._internal import config
from ovirt_imageio._internal import errors
from ovirt_imageio._internal import ops
from ovirt_imageio._internal import util
from ovirt_imageio._internal.auth import Ticket, Authorizer

from test import testutil

CHUNK_SIZE = 8 * 1024**2

log = logging.getLogger("test")


class Context:

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


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


@pytest.fixture
def cfg():
    return config.load(["test/conf.d/daemon.conf"])


def test_transfered_nothing(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    assert ticket.transferred() == 0


def test_transfered_inactive_empty_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.run(Operation(0, 0))
    assert ticket.transferred() == 0

    ticket.run(Operation(1000, 0))
    assert ticket.transferred() == 0


def test_transfered_inactive_ordered_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.run(Operation(0, 100))
    assert ticket.transferred() == 100

    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 300


def test_transfered_inactive_unordered_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 100

    ticket.run(Operation(0, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 300


def test_transfered_inactive_overlapping_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.run(Operation(0, 120))
    assert ticket.transferred() == 120

    ticket.run(Operation(100, 100))
    assert ticket.transferred() == 200

    ticket.run(Operation(180, 120))
    assert ticket.transferred() == 300


def test_transfered_inactive_non_continuous_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    # Run 2 non-continutes operations
    ticket.run(Operation(0, 100))
    ticket.run(Operation(200, 100))
    assert ticket.transferred() == 200

    # Run last operation filling the hole - with some overlap.
    ticket.run(Operation(80, 120))
    assert ticket.transferred() == 300


def test_transfered_ongoing_concurrent_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

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


def test_transfered_ongoing_overlapping_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

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


def test_transfered_ongoing_non_continues_ops(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

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


class Client:

    name = "client"
    workers = 1
    io_size = 1024**2

    def run(self):
        start = time.monotonic()
        threads = []
        try:
            for i in range(self.workers):
                t = util.start_thread(self._worker, name=f"{self.name}/{i}")
                threads.append(t)
        finally:
            for t in threads:
                t.join()
        return time.monotonic() - start

    def _worker(self):
        raise NotImplementedError


class Nbdcopy(Client):
    """
    Simulate nbdcopy client calls.

    The image is split to 128 MiB segments, and each worker sends one segment
    at a time to the server.

    Because very worker is sending to a different area, completed ranges are
    always non-continuous, until the segment is completed.

    worker 1:  [=====-      ]
    worker 2:               [=====-      ]
    worker 3:                            [=====-      ]
    worker 4:                                         [=====-      ]

    completed: [=====        =====        =====        =====       ]
    ongoing:   [     -            -            -            -      ]
    """

    name = "nbdcopy"
    segment_size = 128 * 1024**2

    def __init__(self, ticket, workers):
        assert (ticket.size % self.segment_size) == 0
        self.ticket = ticket
        self.image_size = ticket.size
        self.segment_count = self.image_size // self.segment_size
        self.workers = min(workers, self.segment_count)
        self.segment_counter = itertools.count()

    def _worker(self):
        for segment in iter(self._next_segment, None):
            offset = segment * self.segment_size
            log.info("Transferring segment %d offset %d", segment, offset)
            end = offset + self.segment_size
            while offset < end:
                self.ticket.run(Operation(offset, self.io_size))
                offset += self.io_size

    def _next_segment(self):
        n = next(self.segment_counter)
        if n < self.segment_count:
            return n


class Imageio(Client):
    """
    Simulate imageio client calls.

    Workers read extents from a queue and send requests to the server.

    Because all threads are sending to same area, operations are more likely to
    be consecutive, and merged immediately with the previous requests.

    worker 1:  [==      ==      --                                    ]
    worker 2:  [  ==      ==      --                                  ]
    worker 3:  [    ==      ==      --                                ]
    worker 4:  [      ==      ==      --                              ]

    completed: [================                                      ]
    ongoing:   [                --------                              ]
    """

    name = "imageio"

    def __init__(self, ticket, workers):
        assert (ticket.size % self.io_size) == 0
        self.ticket = ticket
        self.image_size = ticket.size
        self.workers = workers
        self.request_count = self.image_size // self.io_size
        self.request_counter = itertools.count()

    def _worker(self):
        for request in iter(self._next_request, None):
            offset = request * self.io_size
            self.ticket.run(Operation(offset, self.io_size))

    def _next_request(self):
        n = next(self.request_counter)
        if n < self.request_count:
            return n


@pytest.mark.benchmark
@pytest.mark.parametrize("workers", [1, 2, 4, 8])
@pytest.mark.parametrize("client_class", [Nbdcopy, Imageio],
                         ids=lambda x: x.name)
@pytest.mark.parametrize("mode", [
    pytest.param(["read"], id="ro"),
    pytest.param(["read", "write"], id="rw"),
])
def test_run_benchmark(cfg, workers, client_class, mode):
    ticket = Ticket(
        testutil.create_ticket(
            size=100 * 1024**3,
            ops=mode),
        cfg)

    client = client_class(ticket, workers)

    elapsed = client.run()

    if mode != ["read", "write"]:
        assert ticket.transferred() == ticket.size

    ops = ticket.size // client.io_size
    print("%d workers, %d ops, %.3f s, %.2f ops/s"
          % (client.workers, ops, elapsed, ops / elapsed))


@pytest.mark.benchmark
@pytest.mark.parametrize("workers", [1, 2, 4, 8])
def test_transferred_benchmark(cfg, workers):
    # Time trransferred call with multiple ongoing and completed operations.
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

    ops = 10000

    # Add some completed ranges - assume worst case when ranges are not
    # continues.
    for i in range(workers):
        ticket.run(Operation(i * 1000, 100))

    # Add some ongoing operations - assume worst case when ranges are not
    # continues.
    for i in range(workers):
        ticket._add_operation(Operation(i * 1000 + 200, 100))

    # Time transferred call - merging ongoing and completed ranges.
    start = time.monotonic()
    for i in range(ops):
        ticket.transferred()
    elapsed = time.monotonic() - start

    print("%d workers, %d ops, %.3f s, %.2f ops/s"
          % (workers, ops, elapsed, ops / elapsed))


@pytest.mark.parametrize("arg", [
    "not a dict",
    ["not", "a", "dict"],
    1,
    3.1,
    True,
    False,
    None
])
def test_invalid_argument(arg, cfg):
    with pytest.raises(errors.InvalidTicket):
        Ticket(arg, cfg)


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
    {"inactivity_timeout": "invalid"},
])
def test_invalid_parameter(kw, cfg):
    with pytest.raises(errors.InvalidTicketParameter):
        Ticket(testutil.create_ticket(**kw), cfg)


def test_inactivity_timeout_unset(cfg):
    ticket = Ticket(testutil.create_ticket(inactivity_timeout=None), cfg)
    assert ticket.inactivity_timeout == cfg.daemon.inactivity_timeout


def test_sparse_unset(cfg):
    ticket = Ticket(testutil.create_ticket(), cfg)
    assert not ticket.sparse


def test_sparse(cfg):
    ticket = Ticket(testutil.create_ticket(sparse=True), cfg)
    assert ticket.sparse


def test_dirty_unset(cfg):
    ticket = Ticket(testutil.create_ticket(), cfg)
    assert not ticket.dirty


def test_dirty(cfg):
    ticket = Ticket(testutil.create_ticket(dirty=True), cfg)
    assert ticket.dirty


def test_transfer_id_unset(cfg):
    d = testutil.create_ticket()
    del d["transfer_id"]
    ticket = Ticket(d, cfg)
    assert ticket.transfer_id == f"(ticket/{ticket.uuid[:18]})"


def test_transfer_id(cfg):
    ticket = Ticket(testutil.create_ticket(transfer_id="123"), cfg)
    assert ticket.transfer_id == "123"


def test_repr(cfg):
    ticket = Ticket(
        testutil.create_ticket(
            ops=["read"], filename="tmp_file"),
        cfg)
    ticket_repr = repr(ticket)

    info = ticket.info()
    del info["timeout"]

    for key, value in info.items():
        pair = "%s=%r" % (key, value)
        assert pair in ticket_repr


def test_ticket_run(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    op = Operation(0, 100)
    assert ticket.transferred() == op.done
    assert op.done == 0

    ticket.run(op)

    assert ticket.transferred() == op.done
    assert op.done == 100


def test_cancel_no_connection(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.cancel()

    # Ticket is canceled and can be removed immediately.
    assert ticket.canceled
    info = ticket.info()
    assert info["canceled"]
    assert not info["active"]
    assert info["connections"] == 0


def test_cancel_idle_connection(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ctx = Context()
    ticket.add_context(1, ctx)
    ticket.cancel()

    # Ticket is canceled and can be removed immediately.
    assert ticket.canceled
    assert ctx.closed

    info = ticket.info()
    assert info["canceled"]
    assert not info["active"]

    # The conection context was closed. The connection will be closed when it
    # times out or when a user send the next request.
    assert info["connections"] == 1


def test_cancel_timeout(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

    # Add conection - having connections does not block cancelation, but we
    # cannot have ongoing operations without a connection.
    ctx = Context()
    ticket.add_context(1, ctx)

    # Ongoing operation blocks cancel.
    ticket._add_operation(Operation(0, 100))

    # Canceling will time out.
    with pytest.raises(errors.TransferCancelTimeout):
        ticket.cancel(timeout=0.001)

    # Ticket is marked as canceled, but the context was not closed.
    assert ticket.canceled
    assert not ctx.closed

    # Caller can poll ticket "active" property and remove the ticket when the
    # ticket is inactive.
    info = ticket.info()
    assert info["canceled"]
    assert info["active"]
    assert info["connections"] == 1


def test_cancel_async(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ctx = Context()
    ticket.add_context(1, ctx)
    ticket._add_operation(Operation(0, 100))
    ticket.cancel(timeout=0)

    # Ticket is marked as canceled, but the context was not closed.
    assert ticket.canceled
    assert not ctx.closed

    info = ticket.info()
    assert info["canceled"]
    assert info["active"]
    assert info["connections"] == 1


def test_cancel_ongoing_operations(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

    # Few connections are using this ticket. Each running an operation.
    ops = []
    for i in range(4):
        ctx = Context()
        op = Operation()
        ticket.add_context(i, ctx)
        ticket._add_operation(op)
        ops.append(op)

    # Add idle connection.
    idle_ctx = Context()
    ticket.add_context(4, idle_ctx)

    ticket.cancel(timeout=0)

    # All ongoing operations are canceled.
    assert all(op.canceled for op in ops)

    # Idle context was not closed.
    assert not idle_ctx.closed


def test_cancel_wait(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

    # Add connections using this ticket.
    users = []
    for cid in range(4):
        ctx = Context()
        op = Operation(cid * 1024**2, 1024**2)
        ticket.add_context(cid, ctx)
        ticket._add_operation(op)
        users.append((cid, ctx, op))

    # Add idle connection.
    idle_ctx = Context()
    ticket.add_context(4, idle_ctx)

    def finish_operations():
        time.sleep(0.1)
        for cid, ctx, op in users:
            # Removing operation from a canceled ticket raises, send and error
            # and close the connection.
            try:
                ticket._remove_operation(op)
            except errors.AuthorizationError:
                ticket.remove_context(cid)

    info = ticket.info()
    assert not info["canceled"]
    assert info["connections"] == 5
    assert info["active"]

    t = util.start_thread(finish_operations)
    try:
        ticket.cancel(timeout=10)
    finally:
        t.join()

    info = ticket.info()

    # After the ticket was canceled, ticket is inactive, and all ongoging
    # connnections removed from ticket. The idle connection is left, but its
    # context is closed.

    assert not info["active"]
    assert info["connections"] == 1
    assert all(ctx.closed for cid, ctx, op in users)
    assert idle_ctx.closed


def test_canceled_fail_run_before(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.cancel()

    op = Operation()

    # Running operations must fail.
    with pytest.raises(errors.AuthorizationError):
        ticket.run(op)

    # Operation was not run.
    assert op.done == 0


def test_canceled_fail_run_after(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)

    class Operation:

        def __init__(self):
            self.done = False
            self.canceled = False

        def run(self):
            self.done = True
            ticket.cancel(timeout=0.001)

        def cancel(self):
            self.canceled = True

    op = Operation()

    # If ticket was canceled while ongoing operations are running, ticket run
    # will fail removing the operations.
    with pytest.raises(errors.AuthorizationError):
        ticket.run(op)

    assert op.done


def test_canceled_fail_add_context(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.cancel()

    ctx = Context()

    # Adding new context must fail.
    with pytest.raises(errors.AuthorizationError):
        ticket.add_context(2, ctx)


def test_get_context_missing(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    with pytest.raises(KeyError):
        ticket.get_context(1)


def test_get_context(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ctx = Context()
    ticket.add_context(1, ctx)
    assert ticket.get_context(1) is ctx


def test_remove_context_missing(cfg):
    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ticket.add_context(1, Context())
    assert ticket.info()["connections"] == 1

    ticket.remove_context(2)
    assert ticket.info()["connections"] == 1


def test_remove_context_error(cfg):

    class FailingContext:

        def __init__(self):
            self.count = 1
            self.closed = False

        def close(self):
            if self.count > 0:
                self.count -= 1
                raise RuntimeError("Cannot close yet")
            self.closed = True

    ticket = Ticket(testutil.create_ticket(ops=["read"]), cfg)
    ctx = FailingContext()
    ticket.add_context(1, ctx)

    # If closing a context fails, fail. The ticket cannot be removed
    # until this context is closed successfully.
    with pytest.raises(RuntimeError):
        ticket.cancel(timeout=0)

    assert not ctx.closed

    # Calling again will close context successfully, and the ticket can
    # be removed.
    ticket.cancel(timeout=0)

    assert ctx.closed


def test_authorizer_add(cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    assert ticket.uuid == ticket_info["uuid"]


def test_authorizer_remove_unused(cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    # Ticket is unused so it will be removed.
    auth.remove(ticket_info["uuid"])
    with pytest.raises(KeyError):
        auth.get(ticket_info["uuid"])


def test_authorizer_remove_timeout(cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])

    ctx = Context()
    ticket.add_context(1, ctx)

    idle_ctx = Context()
    ticket.add_context(2, idle_ctx)

    assert ticket.info()["connections"] == 2
    assert not ticket.info()["active"]

    op = Operation(0, 100)
    ticket._add_operation(op)
    assert ticket.info()["active"]

    # Use short timeout to keep the tests fast.
    cfg.control.remove_timeout = 0.001

    # Ticket cannot be removed since it is used by connection 1.
    with pytest.raises(errors.TransferCancelTimeout):
        auth.remove(ticket.uuid)

    # Ticket was not removed.
    assert auth.get(ticket.uuid) is ticket

    # But was canceled.
    info = ticket.info()
    assert info["canceled"]
    assert info["active"]
    assert info["connections"] == 2

    # Contexts not closed.
    assert not ctx.closed
    assert not idle_ctx.closed

    # Ending the operation makes the ticket inactive. The call raise and
    # error handller close the connection, which remove the contenxt
    # from the ticket.
    try:
        ticket._remove_operation(op)
    except errors.AuthorizationError:
        ticket.remove_context(1)

    info = ticket.info()
    assert info["canceled"]
    assert not info["active"]
    assert info["connections"] == 1
    assert ctx.closed

    # Idle context not closed yet.
    assert not idle_ctx.closed

    # Removing the ticket again close the idle context.
    auth.remove(ticket.uuid)
    assert idle_ctx.closed

    # Ticket was removed.
    with pytest.raises(KeyError):
        auth.get(ticket.uuid)


def test_authorizer_remove_async(cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    ctx = Context()
    ticket.add_context(1, ctx)

    idle_ctx = Context()
    ticket.add_context(2, idle_ctx)

    assert not ticket.info()["active"]

    op = Operation(0, 100)
    ticket._add_operation(op)
    assert ticket.info()["active"]

    # Disable the timeout, so removing a ticket cancel the ticket
    # without waiting, and requiring polling the ticket status.
    cfg.control.remove_timeout = 0

    auth.remove(ticket.uuid)

    # Ticket is canceled, but not removed.
    assert ticket.canceled
    assert auth.get(ticket.uuid) is ticket
    info = ticket.info()
    assert info["active"]
    assert info["connections"] == 2
    assert not ctx.closed
    assert not idle_ctx.closed

    # Ending the operation makes the ticket inactive. The call raise and
    # error handller close the connection, which remove the contenxt
    # from the ticket.
    try:
        ticket._remove_operation(op)
    except errors.AuthorizationError:
        ticket.remove_context(1)

    info = ticket.info()
    assert info["canceled"]
    assert not info["active"]
    assert info["connections"] == 1
    assert ctx.closed

    # Idle context not closed yet.
    assert not idle_ctx.closed

    # Removing the ticket again close the idle context.
    auth.remove(ticket.uuid)
    assert idle_ctx.closed

    # Ticket was removed.
    with pytest.raises(KeyError):
        auth.get(ticket.uuid)


def test_authorizer_remove_mising(cfg):
    auth = Authorizer(cfg)
    # Removing missing ticket does not raise.
    auth.remove("no-such-ticket")


def test_authorize_read(cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["read"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    assert auth.authorize(ticket.uuid, "read") == ticket

    with pytest.raises(errors.AuthorizationError):
        auth.authorize(ticket.uuid, "write")


def test_authorize_write(cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["write"])
    auth.add(ticket_info)

    ticket = auth.get(ticket_info["uuid"])
    assert auth.authorize(ticket.uuid, "write") == ticket

    # "write" implies also "read".
    assert auth.authorize(ticket.uuid, "read") == ticket


def test_authorizer_no_ticket(cfg):
    auth = Authorizer(cfg)
    with pytest.raises(errors.AuthorizationError):
        auth.authorize("no-such-ticket", "read")


@pytest.mark.parametrize("ops,allowed", [
    (["read"], ["read"]),
    (["write"], ["read", "write"]),
])
def test_authorizer_canceled(ops, allowed, cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=ops)
    auth.add(ticket_info)
    ticket = auth.get(ticket_info["uuid"])

    # Cancelling the ticket disables any operation.
    ticket.cancel()

    for op in allowed:
        with pytest.raises(errors.AuthorizationError):
            auth.authorize(ticket.uuid, op)


def test_authorizer_expired(cfg):
    auth = Authorizer(cfg)
    ticket_info = testutil.create_ticket(ops=["write"])
    auth.add(ticket_info)
    ticket = auth.get(ticket_info["uuid"])

    # Extending with zero timeout expire the ticket.
    ticket.extend(0)

    for op in ("read", "write"):
        with pytest.raises(errors.AuthorizationError):
            auth.authorize(ticket.uuid, op)
