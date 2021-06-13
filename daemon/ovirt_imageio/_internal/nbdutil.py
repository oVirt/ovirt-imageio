# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
nbdutil - Network Block Device utility functions.

Geting extents
==============

NBD spec is very liberal about returning extents. A client must handle these
cases when handling reply for block status command.

Single extent
-------------

The server is allowed to return extent for any request, regardless of the
actual extents on storage. In this case the client must send more block status
commands to get the rest of the extent for the requested range.

Storage:  [-------------|-------------|---------------|

Request:  [----------------------------------]
Reply:    [-------------]


Request:                [--------------------]
Reply:                  [-------------]

Request:                              [------]
Reply:                                [------]

Result:   [-------------|-------------|------]


Short reply
-----------

The server can return some extents, not covering the entire requested ragne.
This is mostly like single extent case.

Storage:  [-------------|-------------|---------------|

Request:  [----------------------------------]
Reply:    [-------------]-------------]

Request:                              [------]
Reply:                                [------]

Result:   [-------------|-------------|------]


Long reply
----------

When the server return multiple extents (N), the N-1 extent must be within the
requested ragne, but the last extent may exceed it. In this case the client
need to clip the last extent to the requested range.

Storage:  [-------------|-------------|---------------|

Request:  [----------------------------------]
Reply:    [-------------]-------------|---------------]

Result:   [-------------|-------------|------]


Consecutive extents of same type
--------------------------------

The server may return multiple extents of same type for the same extent on
storage. The client need to merge the consecutive extents.

Storage:  [xxxxxxxxxxxxx|0000000000000|xxxxxxxxxxxxxxx|

Request:  [----------------------------------]
Reply:    [xxxxxx|xxxxxx|000000|000000|xxxxxx]

Result:   [xxxxxxxxxxxxx|0000000000000|xxxxxx]

"""

import logging
import queue
import sys

from collections import namedtuple

from . import nbd
from . import util

# NBD spec allows zeroing up to 2**32 - 1 bytes, buf some nbd servers like
# qemu-nbd limit seems to be 2**31 - 512. Large zeros can delay more important
# I/O so we like to zero is smaller steps.
MAX_ZERO = 1024**3

log = logging.getLogger("nbdutil")


def extents(client, offset=0, length=None, dirty=False):
    """
    Iterate over all extents for requested range.

    Requested range must not excceed image size, but is not limited by NBD
    maximum length, since we send multiple block status commands to the server.

    Consecutive extents of same type are merged automatically.

    Return iterator of nbd.Extent objects.
    """
    if length is None:
        end = client.export_size
    else:
        end = offset + length

    # NBD limit extents request to 4 GiB - 1. We use smaller step to limit the
    # number of extents kept in memory when accessing very fragmented images.
    max_step = 2 * 1024**3

    # Keep the current extent, until we find a new extent with different flags.
    cur = None

    while offset < end:
        # Get the next extent reply since the last returned extent. This
        # handles the cases of single extent, short reply, and last extent
        # exceeding requested range.
        step = min(end - offset, max_step)
        res = client.extents(offset, step)

        if dirty:
            extents = merged(
                res[nbd.BASE_ALLOCATION], res[client.dirty_bitmap])
        elif nbd.QEMU_ALLOCATION_DEPTH in res:
            extents = merged(
                res[nbd.BASE_ALLOCATION], res[nbd.QEMU_ALLOCATION_DEPTH])
        else:
            extents = res[nbd.BASE_ALLOCATION]

        for ext in extents:
            # Handle the case of last extent of the last block status command
            # exceeding requested range.
            if offset + ext.length > end:
                ext.length = end - offset

            offset += ext.length

            # Handle the case of consecutive extents with same flags.
            if cur is None:
                cur = ext
            elif cur.flags == ext.flags:
                cur.length += ext.length
            else:
                yield cur
                cur = ext

            # The spec does not allow the server to send more extent. Ensure
            # that we don't report wrong data if the server does not comply.
            if offset == end:
                break

    yield cur


def merged(extents_a, extents_b):
    """
    Merge lists of extents with distinct flags bits, yielding merged extents
    with flags from both lists. Yielding ends when the first iterator is
    consumed.

    Yields nbd.Extent() including all bits from both extents.
    """
    iter_a = iter(extents_a)
    iter_b = iter(extents_b)

    a = None
    b = None

    while True:
        try:
            if a is None:
                a = next(iter_a)
            if b is None:
                b = next(iter_b)
        except StopIteration:
            return

        if a.length == b.length:
            # The easy case, merge and clear both.
            yield nbd.Extent(a.length, a.flags | b.flags)
            a = None
            b = None
        elif a.length > b.length:
            # Yield the overlapping area and keep rest of a.
            yield nbd.Extent(b.length, a.flags | b.flags)
            a = nbd.Extent(a.length - b.length, a.flags)
            b = None
        else:  # a.length < b.length
            # Yield the overlapping area and keep rest of b.
            yield nbd.Extent(a.length, a.flags | b.flags)
            b = nbd.Extent(b.length - a.length, b.flags)
            a = None


def copy(src_client, dst_client, block_size=4 * 1024**2, queue_depth=4,
         progress=None):
    """
    Copy export from src_client to dst_client.

    Both exports must have identical size, but can have different format.
    """

    # Consider both requested block size and clients limits.
    buf_size = min(
        block_size,
        min(src_client.maximum_block_size, dst_client.maximum_block_size))

    # Leave extra room for None buffer signaling that the writer failed.
    buffers = queue.Queue(queue_depth + 1)

    # Allocate buffers for write requests.
    for _ in range(queue_depth):
        buffers.put(bytearray(buf_size))

    # Keep room for queue_depth write requests (have buffer) and
    # queue_depth zero requests (have no buffer).
    requests = queue.Queue(queue_depth * 2)

    error = [None]

    log.debug("starting writer thread")
    writer = util.start_thread(
        _write,
        args=(dst_client, requests, buffers, error, progress),
        name="writer")

    _read(src_client, requests, buffers, error)

    log.debug("waiting for writer thread")
    writer.join()

    if error[0]:
        raise error[0][1]


# Request ops.
WRITE = "write"
ZERO = "zero"
FLUSH = "flush"


class Request(namedtuple("Request", "op,offset,length,buf")):
    __slots__ = ()

    def __new__(cls, op, offset=0, length=0, buf=None):
        return tuple.__new__(cls, (op, offset, length, buf))


def _read(client, requests, buffers, error):
    log.debug("reader started")

    offset = 0
    for ext in extents(client):
        todo = ext.length
        if ext.zero:
            while todo:
                if error[0]:
                    log.debug("reader stopped")
                    return

                step = min(todo, MAX_ZERO)
                requests.put(Request(ZERO, offset, step))
                offset += step
                todo -= step
        else:
            while todo:
                buf = buffers.get()
                if error[0]:
                    log.debug("reader stopped")
                    return

                step = min(todo, len(buf))
                view = memoryview(buf)[:step]
                client.readinto(offset, view)
                requests.put(Request(WRITE, offset, step, buf))
                offset += step
                todo -= step

    requests.put(Request(FLUSH))
    requests.put(None)
    log.debug("reader finished")


def _write(client, requests, buffers, error, progress=None):
    try:
        log.debug("writer started")

        while True:
            req = requests.get()
            if req is None:
                log.debug("writer stopped")
                break

            if req.op is ZERO:
                client.zero(req.offset, req.length)
            elif req.op is WRITE:
                view = memoryview(req.buf)[:req.length]
                client.write(req.offset, view)
                buffers.put(req.buf)
            elif req.op is FLUSH:
                client.flush()
            else:
                raise RuntimeError("Unknown request: {}".format(req))

            if progress:
                progress.update(req.length)

        log.debug("writer finished")
    except Exception:
        log.debug("writer failed")
        error[0] = sys.exc_info()
        buffers.put(None)
