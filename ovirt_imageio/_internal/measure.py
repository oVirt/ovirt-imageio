# ovirt-imageio
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import bisect


class Range:

    __slots__ = ("start", "end")

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __lt__(self, other):
        if self.start < other.start:
            return True
        if self.start == other.start:
            return self.end < other.end
        return False

    def __len__(self):
        return self.end - self.start

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.start == other.start and
                self.end == other.end)

    def __repr__(self):
        return f"Range(start={self.start}, end={self.end})"


class RangeList:

    def __init__(self, other=None):
        if other:
            self._ranges = [Range(r.start, r.end) for r in other._ranges]
        else:
            self._ranges = []

    def add(self, r):
        """
        Add a single range.
        """
        bisect.insort_left(self._ranges, r)
        self._ranges = _merged(self._ranges)

    def update(self, rs):
        """
        Update from iterable of unsorted ranges.
        """
        self._ranges.extend(rs)
        self._ranges.sort()
        self._ranges = _merged(self._ranges)

    def sum(self):
        return sum(len(r) for r in self._ranges)


def _merged(ranges):
    """
    Merge sorted list of ranges.

    The ranges are sorted, but may contain:

    - consecutive ranges (very likely):

      [Range(0, 100), Range(100, 200)]

    - duplicate ranges (unlikely):

      [Range(0, 100), Range(0, 100)]

    - ranges overlapping other ranges (unlikely):

      [Range(0, 100), Range(50, 200)]

    Return a merged list of ranges without duplicates, overlaps, or
    consecutive ranges.
    """
    merged = ranges[:1]

    for r in ranges[1:]:
        current = merged[-1]
        if current.end >= r.start:
            current.end = max(current.end, r.end)
        else:
            merged.append(r)

    return merged
