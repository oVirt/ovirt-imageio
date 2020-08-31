# ovirt-imageio
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.


class Range:

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __lt__(self, other):
        return (self.start, self.end) < (other.start, other.end)

    def __len__(self):
        return self.end - self.start

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.start == other.start and
                self.end == other.end)

    def __ne__(self, other):
        return not self == other


def merge_ranges(ranges):
    """
    Gets an iterable of Range objects and returns
    a sorted list of the merged ranges.
    """
    ranges = sorted(ranges)
    if not ranges:
        return ranges
    merged = [ranges.pop(0)]
    for range in ranges:
        current = merged[-1]
        if current.end >= range.start:
            current.end = max(current.end, range.end)
        else:
            merged.append(range)
    return merged
