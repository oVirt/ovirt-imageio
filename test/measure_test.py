# ovirt-imageio
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

from ovirt_imageio._internal import measure
from ovirt_imageio._internal.measure import Range

# Compare ranges.


def test_range_lt_start_smaller():
    assert Range(5, 10) < Range(6, 10)
    assert not Range(6, 10) < Range(5, 10)


def test_range_lt_start_same():
    assert Range(5, 10) < Range(5, 11)
    assert not Range(5, 11) < Range(5, 10)


def test_range_eq():
    assert Range(5, 10) == Range(5, 10)


def test_ragne_ne():
    assert Range(5, 10) != Range(15, 20)


@pytest.mark.parametrize("orig_ranges,merged_ranges", [
    # No range
    ([], []),

    # Single range
    ([(5, 10)], [(5, 10)]),

    # Two consecutive ranges.
    ([(5, 10), (10, 20)], [(5, 20)]),

    # Two ranges with a "hole" in between.
    ([(5, 10), (15, 20)], [(5, 10), (15, 20)]),

    # Two overlapping ranges.
    ([(5, 10), (5, 20)], [(5, 20)]),

    # Three unsorted and partially overlapping ranges.
    ([(5, 10), (0, 3), (7, 20)],
     [(0, 3), (5, 20)]),

    # Three separate ranges.
    ([(5, 10), (15, 20), (25, 30)],
     [(5, 10), (15, 20), (25, 30)]),

    # Three unsorted and overlapping ranges.
    ([(5, 10), (0, 5), (10, 20)], [(0, 20)]),
])
def test_merge_ranges(orig_ranges, merged_ranges):
    orig_ranges = [Range(start, end) for start, end in orig_ranges]
    merged_ranges = [Range(start, end) for start, end in merged_ranges]
    assert measure.merge_ranges(orig_ranges) == merged_ranges
