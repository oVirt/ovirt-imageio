# ovirt-imageio
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from ovirt_imageio._internal.measure import Range, RangeList

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


# Add to range list


def test_range_list_empty():
    rl = RangeList()
    assert rl.sum() == 0


def test_range_list_add_first():
    rl = RangeList()
    rl.add(Range(0, 10))
    assert rl._ranges == [Range(0, 10)]
    assert rl.sum() == 10


def test_range_list_add_same():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(0, 10))
    assert rl._ranges == [Range(0, 10)]
    assert rl.sum() == 10


def test_range_list_add_shorter():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(0, 5))
    assert rl._ranges == [Range(0, 10)]
    assert rl.sum() == 10


def test_range_list_add_longer():
    rl = RangeList()
    rl.add(Range(0, 5))
    rl.add(Range(0, 10))
    assert rl._ranges == [Range(0, 10)]
    assert rl.sum() == 10


def test_range_list_add_overlap():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(5, 15))
    assert rl._ranges == [Range(0, 15)]
    assert rl.sum() == 15


def test_range_list_add_contiguous():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(10, 20))
    assert rl._ranges == [Range(0, 20)]
    assert rl.sum() == 20


def test_range_list_add_non_contiguous():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(20, 30))
    assert rl._ranges == [Range(0, 10), Range(20, 30)]
    assert rl.sum() == 20


def test_ragne_list_add_overlap_next_some():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(20, 30))
    rl.add(Range(15, 25))
    assert rl._ranges == [Range(0, 10), Range(15, 30)]
    assert rl.sum() == 25


def test_ragne_list_add_overlap_next_all():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(20, 30))
    rl.add(Range(15, 30))
    assert rl._ranges == [Range(0, 10), Range(15, 30)]
    assert rl.sum() == 25


def test_range_list_add_overlap_both_some():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(20, 30))
    rl.add(Range(5, 25))
    assert rl._ranges == [Range(0, 30)]
    assert rl.sum() == 30


def test_range_list_add_overlap_both_all():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(20, 30))
    rl.add(Range(0, 30))
    assert rl._ranges == [Range(0, 30)]
    assert rl.sum() == 30


def test_range_list_add_overlap_next_multi_some():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(20, 30))
    rl.add(Range(40, 50))
    rl.add(Range(60, 70))
    rl.add(Range(25, 45))
    assert rl._ranges == [Range(0, 10), Range(20, 50), Range(60, 70)]
    assert rl.sum() == 50


def test_range_list_add_overlap_next_multi_all():
    rl = RangeList()
    rl.add(Range(0, 10))
    rl.add(Range(20, 30))
    rl.add(Range(40, 50))
    rl.add(Range(60, 70))
    rl.add(Range(20, 50))
    assert rl._ranges == [Range(0, 10), Range(20, 50), Range(60, 70)]
    assert rl.sum() == 50


# Update range list.


def test_range_list_update_first():
    rl = RangeList()
    rl.update([Range(0, 10), Range(10, 20), Range(20, 30), Range(30, 40)])
    assert rl._ranges == [Range(0, 40)]
    assert rl.sum() == 40


def test_range_list_update_same():
    rl = RangeList()
    rl.add(Range(0, 40))
    rl.update([Range(0, 10), Range(10, 20), Range(20, 30), Range(30, 40)])
    assert rl._ranges == [Range(0, 40)]
    assert rl.sum() == 40


def test_range_list_update_contiguous():
    rl = RangeList()
    rl.update([Range(0, 10), Range(100, 110), Range(200, 210)])
    rl.update([Range(10, 20), Range(110, 120), Range(210, 220)])
    assert rl._ranges == [Range(0, 20), Range(100, 120), Range(200, 220)]
    assert rl.sum() == 60


def test_range_list_update_non_contiguous():
    rl = RangeList()
    rl.update([Range(0, 10), Range(100, 110), Range(200, 210)])
    rl.update([Range(300, 310), Range(400, 410), Range(500, 510)])
    assert rl._ranges == [
        Range(0, 10),
        Range(100, 110),
        Range(200, 210),
        Range(300, 310),
        Range(400, 410),
        Range(500, 510),
    ]
    assert rl.sum() == 60


def test_ragne_list_update_overlap_next_some():
    rl = RangeList()
    rl.update([Range(0, 10), Range(20, 30)])
    rl.update([Range(15, 25)])
    assert rl._ranges == [Range(0, 10), Range(15, 30)]
    assert rl.sum() == 25


def test_ragne_list_update_overlap_next_all():
    rl = RangeList()
    rl.update([Range(0, 10), Range(20, 30)])
    rl.update([Range(15, 30)])
    assert rl._ranges == [Range(0, 10), Range(15, 30)]
    assert rl.sum() == 25


def test_range_list_update_overlap_both_some():
    rl = RangeList()
    rl.update([Range(0, 10), Range(20, 30)])
    rl.update([Range(5, 25)])
    assert rl._ranges == [Range(0, 30)]
    assert rl.sum() == 30


def test_range_list_update_overlap_both_all():
    rl = RangeList()
    rl.update([Range(0, 10), Range(20, 30)])
    rl.update([Range(0, 30)])
    assert rl._ranges == [Range(0, 30)]
    assert rl.sum() == 30


def test_range_list_update_overlap_next_multi_some():
    rl = RangeList()
    rl.update([Range(0, 10), Range(20, 30), Range(40, 50), Range(60, 70)])
    rl.update([Range(5, 35), Range(25, 65)])
    assert rl._ranges == [Range(0, 70)]
    assert rl.sum() == 70


def test_range_list_update_overlap_next_multi_all():
    rl = RangeList()
    rl.update([Range(0, 10), Range(20, 30), Range(40, 50), Range(60, 70)])
    rl.update([Range(0, 50), Range(20, 70)])
    assert rl._ranges == [Range(0, 70)]
    assert rl.sum() == 70


# Copy range list.


def test_range_list_copy():
    r1 = RangeList()

    r1.add(Range(0, 100))
    r1.add(Range(200, 300))
    r1.add(Range(400, 500))

    r2 = RangeList(r1)

    # r1 and r2 are qqual.
    assert r1._ranges == r2._ranges
    assert r1.sum() == r2.sum()

    # But independent.
    r1.add(Range(300, 400))
    r2.add(Range(100, 200))

    assert r1._ranges == [Range(0, 100), Range(200, 500)]
    assert r2._ranges == [Range(0, 300), Range(400, 500)]
