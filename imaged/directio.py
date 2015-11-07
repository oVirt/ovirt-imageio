# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from contextlib import contextmanager, closing
import fcntl
import mmap
import os

# This value is used by vdsm when copying image data using dd. Smaller values
# save memory, and larger values minimize syscall and python calls overhead.
BLOCKSIZE = 1024 * 1024


def copy_from_image(path, dst, size, blocksize=BLOCKSIZE):
    """
    Copy size bytes from path to dst fileobject.
    """
    with _open(path, "r") as src:
        todo = size
        with aligned_buffer(blocksize) as block:
            # socket._fileobject returned from socket.socket.makefile() write()
            # cannot handle mmap object, unlike the platform underlying file
            # object, so wrap it with a buffer.
            buf = buffer(block)
            while todo >= blocksize:
                n = src.readinto(block)
                if n == 0:
                    raise Exception("Partial content")
                todo -= n
                dst.write(buf)
                dst.flush()
        if todo:
            disable_directio(src.fileno())
            buf = src.read(todo)
            if not buf:
                raise Exception("Partial content")
            dst.write(buf)
            dst.flush()


def copy_to_image(path, src, size, blocksize=BLOCKSIZE):
    """
    Copy size bytes from src fileobject to path.
    """
    with _open(path, "w") as dst:
        todo = size
        with aligned_buffer(blocksize) as block:
            while todo >= blocksize:
                # socket._fileobject returned from socket.socket.makefile()
                # does not implement readinto(mmap), so we must copy the data
                # into the mmap.
                buf = src.read(blocksize)
                if not buf:
                    raise Exception("Partial content")
                todo -= len(buf)
                block[:] = buf
                dst.write(block)
        if todo:
            disable_directio(dst.fileno())
            buf = src.read(todo)
            if not buf:
                raise Exception("Partial content")
            dst.write(buf)
            os.fdatasync(dst.fileno())


def _open(path, mode="r"):
    if mode == "r":
        flags = os.O_RDONLY
    elif mode == "w":
        flags = os.O_WRONLY
    else:
        raise ValueError("Unsupported mode %r", mode)
    fd = os.open(path, flags | os.O_DIRECT)
    try:
        return os.fdopen(fd, mode + "b", 0)
    except Exception:
        os.close(fd)
        raise


@contextmanager
def aligned_buffer(size):
    buf = mmap.mmap(-1, size, mmap.MAP_PRIVATE)
    with closing(buf):
        yield buf


def disable_directio(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~os.O_DIRECT)
