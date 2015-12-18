# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from contextlib import contextmanager, closing
import fcntl
import io
import mmap
import os

from . import util
from . import errors

# This value is used by vdsm when copying image data using dd. Smaller values
# save memory, and larger values minimize syscall and python calls overhead.
BLOCKSIZE = 1024 * 1024


def copy_from_image(path, dst, size, blocksize=BLOCKSIZE):
    """
    Copy size bytes from path to dst fileobject.
    """
    with io.FileIO(path, "r") as src, aligned_buffer(blocksize) as buf:
        enable_directio(src.fileno())
        todo = size
        while todo:
            if src.tell() % 512:
                raise errors.PartialContent(size, size - todo)
            count = util.uninterruptible(src.readinto, buf)
            if count == 0:
                raise errors.PartialContent(size, size - todo)
            count = min(count, todo)
            dst.write(buffer(buf, 0, count))
            todo -= count


def copy_to_image(path, src, size, blocksize=BLOCKSIZE):
    """
    Copy size bytes from src fileobject to path.

    socket._fileobject returned from socket.socket.makefile() does not
    implement readinto(), so we must read unaligned chunks and copy into the
    aligned buffer.
    """
    with io.FileIO(path, "r+") as dst, aligned_buffer(blocksize) as buf:
        enable_directio(dst.fileno())
        todo = size
        while todo:
            count = min(todo, blocksize)
            chunk = src.read(count)
            if len(chunk) < count:
                raise errors.PartialContent(size, size - todo + len(chunk))
            buf[:count] = chunk
            if count % 512:
                disable_directio(dst.fileno())
            towrite = count
            while towrite:
                offset = count - towrite
                towrite -= util.uninterruptible(dst.write, buffer(buf, offset, count))
            todo -= count
        os.fsync(dst.fileno())


@contextmanager
def aligned_buffer(size):
    """
    Return buffer aligned to 512 bytes, required for doing direct io using
    mmap().

    Note: we use shared map to make direct io safe if fork is invoked in
    another thread concurrently with the direct io.

    Using private maps with direct io can cause data corruption and undefind
    behavior in the parent or the child processes. This restriction does not
    apply to memory buffer created with MAP_SHARED. See open(2) for more info.
    """
    buf = mmap.mmap(-1, size, mmap.MAP_SHARED)
    with closing(buf):
        yield buf


def enable_directio(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_DIRECT)


def disable_directio(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~os.O_DIRECT)
