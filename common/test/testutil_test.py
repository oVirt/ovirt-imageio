# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from . import testutil


def test_unbuffered_stream_more():
    chunks = [b"1" * 256,
              b"2" * 256,
              b"3" * 42,
              b"4" * 256]
    s = testutil.UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(512)
    assert b == chunks[0]
    # Chunk 2
    b = s.read(512)
    assert b == chunks[1]
    # Chunk 3
    b = s.read(512)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(512)
    assert b == chunks[3]
    # Empty
    b = s.read(512)
    assert b == b''
    b = s.read(512)
    assert b == b''


def test_unbuffered_stream_less():
    chunks = [b"1" * 256,
              b"2" * 256,
              b"3" * 42,
              b"4" * 256]
    s = testutil.UnbufferedStream(chunks)
    # Chunk 1
    b = s.read(128)
    assert b == chunks[0][:128]
    b = s.read(128)
    assert b == chunks[0][128:]
    # Chunk 2
    b = s.read(128)
    assert b == chunks[1][:128]
    b = s.read(128)
    assert b == chunks[1][128:]
    # Chunk 3
    b = s.read(128)
    assert b == chunks[2]
    # Chunk 4
    b = s.read(128)
    assert b == chunks[3][:128]
    b = s.read(128)
    assert b == chunks[3][128:]
    # Empty
    b = s.read(128)
    assert b == b''
    b = s.read(128)
    assert b == b''
