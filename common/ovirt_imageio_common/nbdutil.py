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

from __future__ import absolute_import


def extents(client, offset=0, length=None):
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

    # Keep the current extent, until we find a new extent with different zero
    # value.
    cur = None

    while offset < end:
        # Get the next extent reply since the last returned extent. This
        # handles the cases of single extent, short reply, and last extent
        # exceeding requested range.
        step = min(end - offset, max_step)
        res = client.extents(offset, step)

        for ext in res["base:allocation"]:
            # Handle the case of last extent of the last block status command
            # exceeding requested range.
            if offset + ext.length > end:
                ext.length = end - offset

            offset += ext.length

            # Handle the case of consecutive extents with same zero value.
            if cur is None:
                cur = ext
            elif cur.zero == ext.zero:
                cur.length += ext.length
            else:
                yield cur
                cur = ext

            # The spec does not allow the server to send more extent. Ensure
            # that we don't report wrong data if the server does not comply.
            if offset == end:
                break

    yield cur
