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

from ovirt_imageio._internal.nbd import (
    STATE_HOLE,
    STATE_ZERO,
    EXTENT_DIRTY,
    EXTENT_BACKING,
)

MiB = 1024**2
GiB = 1024**3


def extents_length(extents):
    return sum(e.length for e in extents)


class FakeClient:

    def __init__(self, alloc, depth=None, dirty=None, max_extents=None):
        """
        alloc, depth, and dirty are list of extents of same length. The export
        size is set to the length of the extents.

        max_extents is maximum number of extents return in one extents() call
        per meta contenxt.
        """
        # Check extents total length matches.
        alloc_length = extents_length(alloc)
        if depth:
            assert extents_length(depth) == alloc_length
        if dirty:
            assert extents_length(dirty) == alloc_length

        self.export_size = alloc_length
        self.alloc = alloc
        self.depth = depth
        self.dirty = dirty
        self.max_extents = max_extents

        if self.dirty:
            self.dirty_bitmap = nbd.QEMU_DIRTY_BITMAP + "name"
        else:
            self.dirty_bitmap = None

    def extents(self, offset, length):
        """
        Simulate real NBD server extents reply.

        Return extents overlapping the requested range. The first and last
        extents may be clipped to the requested range.

        If max_extents is set, may return short reply not coverting the entire
        requested range. In this case the length of differnet meta context may
        be different.

        If export_size is shorter than the configured extents, the last extent
        may exceed the export size.
        """
        assert length > 0
        assert length <= nbd.MAX_LENGTH

        res = {
            nbd.BASE_ALLOCATION: list(
                self.lookup(offset, length, self.alloc))
        }

        if self.depth:
            res[nbd.QEMU_ALLOCATION_DEPTH] = list(
                self.lookup(offset, length, self.depth))

        if self.dirty:
            res[self.dirty_bitmap] = list(
                self.lookup(offset, length, self.dirty))

        return res

    def lookup(self, offset, length, extents):
        end = offset + length
        start = 0
        count = 0

        for e in extents:
            # Skip before the requested range:
            #   request:       [             ]
            #   extent:   |----|
            if start + e.length <= offset:
                start += e.length
                continue

            length = e.length

            # Clip extent before offset:
            #   request:       [             ]
            #   extent:    |-------|
            #   result:        |===|
            if start < offset:
                clip = offset - start
                length -= clip
                start += clip

            # Clip extent after end:
            #   request:   [             ]
            #   extent:             |-------|
            #   result:             |====|
            if start + length > end:
                clip = start + length - end
                length -= clip

            yield nbd.Extent(length, e.flags)

            # NBD server is allowed to return short reply with one or more
            # extents.
            count += 1
            if self.max_extents and count == self.max_extents:
                break

            start += length

            # Stop lookup after the requested range:
            #   request:  [             ]
            #   extent:                 |----|
            if start >= end:
                break


def fake_client(n, max_extents=0):
    """
    A client simulating few interesting cases:
    - 3 alloction types: data, zero cluster, and unallocated extent.
    - dirty extents convering both data and zero cluster.
    - extents of different meta context of different length.
    - server returning short reply.
    """
    return FakeClient(
        alloc=[
            nbd.Extent(2 * n, 0),
            nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
            nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
        ],
        depth=[
            nbd.Extent(3 * n, 0),               # depth=1
            nbd.Extent(1 * n, 0),               # depth=2
            nbd.Extent(2 * n, EXTENT_BACKING),  # depth=0
        ],
        dirty=[
            nbd.Extent(1 * n, 0),
            nbd.Extent(3 * n, EXTENT_DIRTY),
            nbd.Extent(2 * n, 0)
        ],
        max_extents=max_extents,
    )


# Testing FakeClient


def test_fake_client_simple():
    n = MiB
    c = fake_client(n)
    res = c.extents(0, 6 * n)
    assert res == {
        nbd.BASE_ALLOCATION: c.alloc,
        nbd.QEMU_ALLOCATION_DEPTH: c.depth,
        c.dirty_bitmap: c.dirty,
    }


def test_fake_client_clip_start():
    n = MiB
    c = fake_client(n)
    res = c.extents(n, 5 * n)
    assert res == {
        nbd.BASE_ALLOCATION: [
            nbd.Extent(1 * n, 0),
            nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
            nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
        ],
        nbd.QEMU_ALLOCATION_DEPTH: [
            nbd.Extent(2 * n, 0),               # depth=1
            nbd.Extent(1 * n, 0),               # depth=2
            nbd.Extent(2 * n, EXTENT_BACKING),  # depth=0
        ],
        c.dirty_bitmap: [
            nbd.Extent(3 * n, EXTENT_DIRTY),
            nbd.Extent(2 * n, 0)
        ],
    }


def test_fake_client_clip_end():
    n = MiB
    c = fake_client(n)
    res = c.extents(0, 5 * n)
    assert res == {
        nbd.BASE_ALLOCATION: [
            nbd.Extent(2 * n, 0),
            nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
            nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE),
        ],
        nbd.QEMU_ALLOCATION_DEPTH: [
            nbd.Extent(3 * n, 0),               # depth=1
            nbd.Extent(1 * n, 0),               # depth=2
            nbd.Extent(1 * n, EXTENT_BACKING),  # depth=0
        ],
        c.dirty_bitmap: [
            nbd.Extent(1 * n, 0),
            nbd.Extent(3 * n, EXTENT_DIRTY),
            nbd.Extent(1 * n, 0)
        ],
    }


def test_fake_client_clip_both():
    n = MiB
    c = fake_client(n)
    res = c.extents(2 * n,  2 * n)
    assert res == {
        nbd.BASE_ALLOCATION: [
            nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
        ],
        nbd.QEMU_ALLOCATION_DEPTH: [
            nbd.Extent(1 * n, 0),               # depth=1
            nbd.Extent(1 * n, 0),               # depth=2
        ],
        c.dirty_bitmap: [
            nbd.Extent(2 * n, EXTENT_DIRTY),
        ],
    }


def test_fake_client_max_extents():
    n = MiB
    c = fake_client(n, max_extents=1)
    res = c.extents(0, 6 * n)
    assert res == {
        nbd.BASE_ALLOCATION: [
            nbd.Extent(2 * n, 0),
        ],
        nbd.QEMU_ALLOCATION_DEPTH: [
            nbd.Extent(3 * n, 0),               # depth=1
        ],
        c.dirty_bitmap: [
            nbd.Extent(1 * n, 0),
        ],
    }

    c = fake_client(n, max_extents=2)
    res = c.extents(n, 4 * n)
    assert res == {
        nbd.BASE_ALLOCATION: [
            nbd.Extent(1 * n, 0),
            nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
        ],
        nbd.QEMU_ALLOCATION_DEPTH: [
            nbd.Extent(2 * n, 0),               # depth=1
            nbd.Extent(1 * n, 0),               # depth=2
        ],
        c.dirty_bitmap: [
            nbd.Extent(3 * n, EXTENT_DIRTY),
            nbd.Extent(1 * n, 0)
        ],
    }


# Testing nbdutil.extents()


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_all(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c))
    assert extents == [
        nbd.Extent(2 * n, 0),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE | EXTENT_BACKING),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_all_dirty(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c, dirty=True))
    assert extents == [
        nbd.Extent(1 * n, 0),
        nbd.Extent(1 * n, EXTENT_DIRTY),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE | EXTENT_DIRTY),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_all_no_depth(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)

    # Simulate the case when server does not report allocation depth.
    c.depth = None

    extents = list(nbdutil.extents(c))
    assert extents == [
        nbd.Extent(2 * n, 0),
        nbd.Extent(4 * n, STATE_ZERO | STATE_HOLE),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_offset(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c, offset=3 * n))
    assert extents == [
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE | EXTENT_BACKING),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_offset_dirty(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c, offset=3 * n, dirty=True))
    assert extents == [
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE | EXTENT_DIRTY),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_length(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c, length=3 * n))
    assert extents == [
        nbd.Extent(2 * n, 0),
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_length_dirty(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c, length=3 * n, dirty=True))
    assert extents == [
        nbd.Extent(1 * n, 0),
        nbd.Extent(1 * n, EXTENT_DIRTY),
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE | EXTENT_DIRTY),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_offset_length(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c, offset=n, length=4 * n))
    assert extents == [
        nbd.Extent(1 * n, 0),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE | EXTENT_BACKING),
    ]


@pytest.mark.parametrize("max_extents", [None, 1, 2])
def test_extents_offset_length_dirty(max_extents):
    n = GiB
    c = fake_client(n, max_extents=max_extents)
    extents = list(nbdutil.extents(c, offset=n, length=4 * n, dirty=True))
    assert extents == [
        nbd.Extent(1 * n, EXTENT_DIRTY),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE | EXTENT_DIRTY),
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE),
    ]


def test_extents_last_extent_excceeds_export_size():
    n = GiB
    c = fake_client(n)

    # Clip export size so we get extra extent info exceeding the request
    # length.
    c.export_size -= GiB

    # Merge base:allocation and qemu:allocation-depth.
    extents = list(nbdutil.extents(c))
    assert extents == [
        nbd.Extent(2 * n, 0),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE),
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE | EXTENT_BACKING),
    ]


def test_extents_last_extent_excceeds_export_size_dirty():
    n = GiB
    c = fake_client(n)

    # Clip export size so we get extra extent info exceeding the request
    # length.
    c.export_size -= GiB

    extents = list(nbdutil.extents(c, dirty=True))
    assert extents == [
        nbd.Extent(1 * n, 0),
        nbd.Extent(1 * n, EXTENT_DIRTY),
        nbd.Extent(2 * n, STATE_ZERO | STATE_HOLE | EXTENT_DIRTY),
        nbd.Extent(1 * n, STATE_ZERO | STATE_HOLE),
    ]


# Testing nbdutil.merged()


def test_merge_simple():
    n = GiB
    a = [nbd.Extent(n, 0)]
    b = [nbd.Extent(n, 0)]

    merged = list(nbdutil.merged(a, b))
    assert merged == a


def test_merge_split_one():
    n = GiB
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
    n = GiB
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
    n = GiB
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
