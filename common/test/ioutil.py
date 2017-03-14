# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import collections


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
