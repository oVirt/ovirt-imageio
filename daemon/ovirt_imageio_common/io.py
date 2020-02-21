# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
io - I/O operations on backends.
"""

from __future__ import absolute_import

import logging

# Limit maximum zero and copy size to ensure frequent progress updates when
# handling large extents.
MAX_ZERO_SIZE = 1024**3
MAX_COPY_SIZE = 128 * 1024**2

# NBD hard limit.
MAX_BUFFER_SIZE = 32 * 1024**2

# TODO: Needs testing.
BUFFER_SIZE = 4 * 1024**2

log = logging.getLogger("io")


def copy(src, dst, dirty=False, buffer_size=BUFFER_SIZE, zero=True,
         progress=None):
    buffer_size = min(buffer_size, MAX_BUFFER_SIZE)
    buf = bytearray(buffer_size)

    if dirty:
        _copy_dirty(src, dst, buf, progress)
    else:
        _copy_data(src, dst, buf, zero, progress)

    dst.flush()


def _copy_data(src, dst, buf, zero, progress):
    for ext in src.extents("zero"):
        if not ext.zero:
            _copy_extent(src, dst, ext, buf, progress)
        elif zero:
            _zero_extent(dst, ext, progress)
        elif progress:
            progress.update(ext.length)


def _copy_dirty(src, dst, buf, progress):
    for ext in src.extents("dirty"):
        if ext.dirty:
            _copy_extent(src, dst, ext, buf, progress)
        elif progress:
            progress.update(ext.length)


def _copy_extent(src, dst, ext, buf, progress):
    for start, length in _split(ext.start, ext.length, MAX_COPY_SIZE):
        src.seek(start)
        dst.seek(start)

        if hasattr(dst, "read_from"):
            dst.read_from(src, length, buf)
        elif hasattr(src, "write_to"):
            src.write_to(dst, length, buf)
        else:
            _generic_copy(src, dst, start, length, buf)

        if progress:
            progress.update(length)


def _zero_extent(dst, ext, progress):
    # TODO: Assumes complete zero(); works with the nbd and http backends but
    # not with the file backend.
    for start, length in _split(ext.start, ext.length, MAX_ZERO_SIZE):
        dst.seek(start)
        dst.zero(length)
        if progress:
            progress.update(length)


def _split(start, length, max_length):
    """
    Split big range to smaller ones.
    """
    while length > max_length:
        yield start, max_length
        length -= max_length
        start += max_length

    yield start, length


def _generic_copy(src, dst, start, length, buf):
    # TODO: Assumes complete readinto() and write(); works with the nbd and
    # http backends but not with the file backend.
    step = len(buf)
    todo = length

    while todo > step:
        src.readinto(buf)
        dst.write(buf)
        todo -= step

    with memoryview(buf)[:todo] as last:
        src.readinto(last)
        dst.write(last)
