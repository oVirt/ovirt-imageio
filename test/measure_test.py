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


class TestRange:

    """
    To make the cases visual, each test case is described using two ranges:
    - The range that change in each test case is drawn as follows: "*****".
    - The range that we compare the others to is drawn as follows: "-----".
    """
    @pytest.mark.parametrize("start,end,expected", [
        #          *****
        #     -----
        (15, 20, True),

        #         *****
        #     -----
        (10, 20, True),

        #         *
        #     -----
        (10, 10, True),

        #       *****
        #     -----
        (7, 20, True),

        #       ***
        #     -----
        (7, 10, True),

        #      ***
        #     -----
        (7, 8, True),

        #     *******
        #     -----
        (5, 15, True),

        #     *****
        #     -----
        (5, 10, False),

        #     ***
        #     -----
        (5, 8, False),

        #     *
        #     -----
        (5, 5, False),

        #   *********
        #     -----
        (3, 15, False),

        #   *******
        #     -----
        (3, 10, False),

        # *****
        #     -----
        (3, 5, False),

        #   *****
        #     -----
        (3, 8, False),

        # ***
        #     -----
        (3, 4, False),
    ])
    def test_lt(self, start, end, expected):
        assert (Range(5, 10) < Range(start, end)) == expected

    def test_eq(self):
        assert Range(5, 10) == Range(5, 10)

    def test_ne(self):
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
