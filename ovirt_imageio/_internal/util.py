# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import collections
import io
import mmap
import os
import threading


def start_thread(func, args=(), kwargs=None, name=None, daemon=True):
    if kwargs is None:
        kwargs = {}
    t = threading.Thread(target=func, args=args, kwargs=kwargs, name=name)
    t.daemon = daemon
    t.start()
    return t


def monotonic_time():
    return os.times()[4]


def humansize(n):
    for unit in ("bytes", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if n < 1024:
            break
        n /= 1024
    return "{:.{precision}f} {}".format(
        n, unit, precision=0 if unit == "bytes" else 2)


def round_up(n, size):
    n = n + size - 1
    return n - (n % size)


def round_down(n, size):
    return n - (n % size)


def aligned_buffer(size):
    """
    Return buffer aligned to page size, which work for doing direct I/O.

    Note: we use shared map to make direct io safe if fork is invoked in
    another thread concurrently with the direct io.

    Using private maps with direct io can cause data corruption and undefined
    behavior in the parent or the child processes. This restriction does not
    apply to memory buffer created with MAP_SHARED. See open(2) for more info.
    """
    return mmap.mmap(-1, size, mmap.MAP_SHARED)


def open(path, mode, direct=True, sync=False):
    """
    Open a file for direct I/O.

    Writing or reading from the file requires an aligned buffer. Only
    readinto() can be used to read from the file.

    Arguments:
        path (str): Filesystem path
        mode (str): One of ("r", "w", "r+"). The file is always opened in
            binary mode. See io.FileIO for more info on available modes.
        direct (bool): Try to minimize cache effects of the I/O to and from
            this file (O_DIRECT).
        sync (bool): Write operations on the file will complete according to
            the requirements of synchronized I/O file integrity completion
            (O_SYNC).
    """
    if mode == "r":
        flags = os.O_RDONLY
    elif mode == "w":
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    elif mode == "r+":
        flags = os.O_RDWR
    else:
        raise ValueError("Unsupported mode %r" % mode)

    if direct:
        flags |= os.O_DIRECT

    if sync:
        flags |= os.O_SYNC

    fd = os.open(path, flags)
    return io.FileIO(fd, mode, closefd=True)


def ensure_text(s, encoding='utf-8', errors='strict'):
    """
    Converts:

      - `str` -> `str`
      - `bytes` -> decoded to `str`

    Based on implementation in six library.
    """
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    elif isinstance(s, str):
        return s
    else:
        raise TypeError("not expecting type '%s'" % type(s))


class UnbufferedStream:
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
