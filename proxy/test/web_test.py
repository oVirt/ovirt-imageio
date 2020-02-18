# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import collections
import io

from ovirt_imageio_proxy import web


class UnbufferedStream(object):
    """
    Unlike regular file object, read may return any amount of bytes up to the
    requested size. This behavior is probably the result of doing one syscall
    per read, without any buffering.

    This stream will break code assuming that read(n) retruns n bytes. This
    assumption is normally true, but not all file-like objects behave in this
    way.

    This simulate libvirt stream behavior used to copy imaged directly from
    libvirt.
    https://libvirt.org/html/libvirt-libvirt-stream.html#virStreamRecv
    """

    def __init__(self, chunks):
        self.chunks = collections.deque(chunks)

    def read(self, size):
        if not self.chunks:
            return b''
        chunk = self.chunks.popleft()
        res = chunk[:size]
        chunk = chunk[size:]
        if chunk:
            self.chunks.appendleft(chunk)
        return res

    def readinto(self, buf):
        chunk = self.read(len(buf))
        buf[:len(chunk)] = chunk
        return len(chunk)


def test_capped_stream_iter():
    stream = io.BytesIO(b"x" * 148*1024)
    max_bytes = 138*1024
    capped_stream = web.CappedStream(stream, max_bytes, buffer_size=128*1024)
    data = b"".join(capped_stream)
    assert data == b"x" * max_bytes


def test_capped_stream_buffer_size():
    stream = io.BytesIO(b"x" * 8192)
    buffer_size = 4096
    capped_stream = web.CappedStream(stream, 5120, buffer_size=buffer_size)
    chunks = list(capped_stream)
    assert chunks == [b"x" * buffer_size, b"x" * 1024]


def test_capped_stream_read_default():
    stream = io.BytesIO(b"x" * 8192)
    buffer_size = 4096
    capped_stream = web.CappedStream(stream, 5120, buffer_size=buffer_size)
    assert capped_stream.read() == b"x" * buffer_size
    assert capped_stream.read() == b"x" * 1024


def test_capped_stream_read_size():
    stream = io.BytesIO(b"x" * 1024)
    capped_stream = web.CappedStream(stream, 768)
    assert capped_stream.read(123) == b"x" * 123


def test_capped_stream_short_reads():
    stream = UnbufferedStream([b"1" * 123, b"2" * 456])
    capped_stream = web.CappedStream(stream, 1024)
    assert capped_stream.read(1024) == b"1" * 123
    assert capped_stream.read(1024) == b"2" * 456
    assert capped_stream.read(1024) == b""
