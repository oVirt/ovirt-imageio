# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import nbdutil


class Client:

    export_size = 6 * 1024**3

    def __init__(self, flags, dirty):
        self.flags = flags
        if dirty:
            self.dirty_bitmap = nbd.QEMU_DIRTY_BITMAP + "bitmap-name"
        else:
            self.dirty_bitmap = None

    def extents(self, offset, length):
        assert 0 < length <= nbd.MAX_LENGTH
        assert offset + length <= self.export_size

        extents = self.reply(offset, length)

        res = {nbd.BASE_ALLOCATION: extents}

        if self.dirty_bitmap:
            res[self.dirty_bitmap] = extents

        return res

    def reply(self):
        raise NotImplementedError


class CompleteReply(Client):
    """
    Return what you asked for.
    """

    def reply(self, offset, length):
        return [nbd.Extent(length, self.flags)]


class SingleExtent(Client):
    """
    Return short reply with single extent until user ask for the last extent.
    """

    def reply(self, offset, length):
        length = min(length, 128 * 1024**2)
        return [nbd.Extent(length, self.flags)]


class ShortReply(Client):
    """
    Return short reply with multiple extents until user ask for the last
    extent.
    """

    def reply(self, offset, length):
        max_extent = 128 * 1024**2

        if length > max_extent:
            length -= max_extent

        extents = []
        while length > max_extent:
            extents.append(nbd.Extent(max_extent, self.flags))
            length -= max_extent

        extents.append(nbd.Extent(length, self.flags))

        return extents


class ExcceedsLength(Client):
    """
    Return length + extra bytes in 2 extents until the caller ask for the last
    extent. The spec does not allow returning one extents exceeding requested
    range.
    """

    def reply(self, offset, length):
        extra = 128 * 1024**2

        if offset + length + extra < self.export_size and length > extra:
            return [
                nbd.Extent(length - extra, self.flags),
                nbd.Extent(2 * extra, self.flags),
            ]
        else:
            return [nbd.Extent(length, self.flags)]


class SomeData(Client):
    """
    Return even extents with flags, odd extents without flags.
    """

    extent_size = 2 * 1024**3

    def reply(self, offset, length):
        index = offset // self.extent_size
        flags = self.flags if index % 2 else 0

        max_length = self.extent_size - (offset % self.extent_size)
        length = min(length, max_length)

        return [nbd.Extent(length, flags)]


OFFSET_PARAMS = [
    pytest.param(0, id="stat to end"),
    pytest.param(2 * 1024**3, id="offset to end"),
]

OFFSET_LENGTH_PARAMS = [
    pytest.param(0, Client.export_size, id="start to export_size"),
    pytest.param(0, 2 * 1024**3, id="head"),
    pytest.param(2 * 1024**3, 2 * 1024**3, id="middle"),
    pytest.param(Client.export_size - 2 * 1024**3, 2 * 1024**3, id="tail"),
]

DIRTY_PARAMS = [False, True]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_complete_reply(dirty):
    c = CompleteReply(1, dirty)
    extents = list(nbdutil.extents(c, dirty=dirty))
    assert extents == [nbd.Extent(c.export_size, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
@pytest.mark.parametrize("offset", OFFSET_PARAMS)
def test_complete_reply_offset(dirty, offset):
    c = CompleteReply(1, dirty)
    extents = list(nbdutil.extents(c, offset=offset, dirty=dirty))
    assert extents == [nbd.Extent(c.export_size - offset, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
@pytest.mark.parametrize("offset,length", OFFSET_LENGTH_PARAMS)
def test_complete_reply_offset_length(dirty, offset, length):
    c = CompleteReply(1, dirty)
    extents = list(nbdutil.extents(
        c, offset=offset, length=length, dirty=dirty))
    assert extents == [nbd.Extent(length, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_single_extent(dirty):
    c = SingleExtent(1, dirty)
    extents = list(nbdutil.extents(c, dirty=dirty))
    assert extents == [nbd.Extent(c.export_size, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
@pytest.mark.parametrize("offset", OFFSET_PARAMS)
def test_single_extent_offset(dirty, offset):
    c = SingleExtent(1, dirty)
    extents = list(nbdutil.extents(c, offset=offset, dirty=dirty))
    assert extents == [nbd.Extent(c.export_size - offset, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
@pytest.mark.parametrize("offset,length", OFFSET_LENGTH_PARAMS)
def test_single_extent_offset_length(dirty, offset, length):
    c = SingleExtent(1, dirty)
    extents = list(nbdutil.extents(
        c, offset=offset, length=length, dirty=dirty))
    assert extents == [nbd.Extent(length, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_short_reply(dirty):
    c = ShortReply(1, dirty)
    extents = list(nbdutil.extents(c, dirty=dirty))
    assert extents == [nbd.Extent(c.export_size, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
@pytest.mark.parametrize("offset", OFFSET_PARAMS)
def test_short_reply_offset(dirty, offset):
    c = ShortReply(1, dirty)
    extents = list(nbdutil.extents(c, offset=offset, dirty=dirty))
    assert extents == [nbd.Extent(c.export_size - offset, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
@pytest.mark.parametrize("offset,length", OFFSET_LENGTH_PARAMS)
def test_short_reply_offset_length(dirty, offset, length):
    c = ShortReply(1, dirty)
    extents = list(nbdutil.extents(
        c, offset=offset, length=length, dirty=dirty))
    assert extents == [nbd.Extent(length, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
@pytest.mark.parametrize("offset,length", OFFSET_LENGTH_PARAMS)
def test_last_extent_exceeds_length(dirty, offset, length):
    c = ExcceedsLength(1, dirty)
    extents = list(nbdutil.extents(
        c, offset=offset, length=length, dirty=dirty))
    assert extents == [nbd.Extent(length, 1)]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_some_data(dirty):
    c = SomeData(1, dirty)
    extents = list(nbdutil.extents(c, dirty=dirty))
    assert extents == [
        nbd.Extent(c.extent_size, 0),
        nbd.Extent(c.extent_size, 1),
        nbd.Extent(c.extent_size, 0),
    ]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_some_data_offset(dirty):
    c = SomeData(1, dirty)
    extents = list(nbdutil.extents(c, offset=0, dirty=dirty))
    assert extents == [
        nbd.Extent(c.extent_size, 0),
        nbd.Extent(c.extent_size, 1),
        nbd.Extent(c.extent_size, 0),
    ]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_some_data_offset_length(dirty):
    c = SomeData(1, dirty)
    extents = list(nbdutil.extents(
        c, offset=0, length=c.export_size, dirty=dirty))
    assert extents == [
        nbd.Extent(c.extent_size, 0),
        nbd.Extent(c.extent_size, 1),
        nbd.Extent(c.extent_size, 0),
    ]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_some_data_offset_unaligned(dirty):
    c = SomeData(1, dirty)
    extents = list(nbdutil.extents(
        c, offset=c.extent_size // 2 * 3, dirty=dirty))
    assert extents == [
        nbd.Extent(c.extent_size // 2, 1),
        nbd.Extent(c.extent_size, 0),
    ]


@pytest.mark.parametrize("dirty", DIRTY_PARAMS)
def test_some_data_offset_length_unaligned(dirty):
    c = SomeData(1, dirty)
    extents = list(nbdutil.extents(
        c,
        offset=c.extent_size // 2,
        length=c.extent_size * 2,
        dirty=dirty))
    assert extents == [
        nbd.Extent(c.extent_size // 2, 0),
        nbd.Extent(c.extent_size, 1),
        nbd.Extent(c.extent_size // 2, 0),
    ]


def test_merge_simple():
    n = 1024**3
    a = [nbd.Extent(n, 0)]
    b = [nbd.Extent(n, 0)]

    merged = list(nbdutil.merged(a, b))
    assert merged == a


def test_merge_split_one():
    n = 1024**3
    a = [
        nbd.Extent(n, 1),
        nbd.Extent(n, 2),
        nbd.Extent(n, 4),
    ]
    b = [
        nbd.Extent(n * 3, 8)
    ]

    merged1 = list(nbdutil.merged(a, b))
    assert merged1 == [
        nbd.Extent(n, 1 | 8),
        nbd.Extent(n, 2 | 8),
        nbd.Extent(n, 4 | 8),
    ]

    merged2 = list(nbdutil.merged(b, a))
    assert merged2 == merged1


def test_merge_split_both():
    n = 1024**3
    a = [
        nbd.Extent(n * 1, 1),
        nbd.Extent(n * 2, 2),
    ]
    b = [
        nbd.Extent(n * 2, 4),
        nbd.Extent(n * 1, 8),
    ]

    merged1 = list(nbdutil.merged(a, b))
    assert merged1 == [
        nbd.Extent(n, 1 | 4),
        nbd.Extent(n, 2 | 4),
        nbd.Extent(n, 2 | 8),
    ]

    merged2 = list(nbdutil.merged(b, a))
    assert merged2 == merged1


def test_merge_clip():
    n = 1024**3
    a = [
        nbd.Extent(n * 1, 1),
        nbd.Extent(n * 1, 2),
    ]
    b = [
        nbd.Extent(n * 1, 4),
        nbd.Extent(n * 2, 8),
    ]

    merged1 = list(nbdutil.merged(a, b))
    assert merged1 == [
        nbd.Extent(n * 1, 1 | 4),
        nbd.Extent(n * 1, 2 | 8),
    ]

    merged2 = list(nbdutil.merged(b, a))
    assert merged2 == merged1
