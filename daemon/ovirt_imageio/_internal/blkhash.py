# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import hashlib
import os
from functools import partial

from . import ioutil

# These settings give best result for Fedora 32 image when using the nbd
# backend. More testing is needed to determine if this is the best default.
BLOCK_SIZE = 4 * 1024**2
ALGORITHM = "blake2b"
DIGEST_SIZE = 32


class Hash:
    """
    Block based hash supporting fast zero block hashing.

    To use this, you must first split the input to block_size legnth blocks.
    Use blkhash.split() to retrun stream of blocks from stream of variable size
    extents.

    Then call update(data) for every data block, and zero(length) for every
    zero block, in the order of the blocks in the file. Zero block hashing is
    optimized by using pre-computed digest instead of hashing zero block.

    If you don't have extents information, split the file to block_size legnth
    blocks and call update(data) in the order of the blcoks. The result will be
    equal but much slower.

    The last block may be shorter if the file is not aligned to block_size.
    """

    def __init__(self, block_size=BLOCK_SIZE, algorithm=ALGORITHM,
                 digest_size=DIGEST_SIZE):
        self._func = getattr(hashlib, algorithm)
        if digest_size:
            self._func = partial(self._func, digest_size=digest_size)
        self._hash = self._func()
        self._block_size = block_size
        self._zero_block_digest = self._func(b"\0" * block_size).digest()

    def update(self, block):
        block_digest = self._func(block).digest()
        self._hash.update(block_digest)

    def zero(self, count):
        if count == self._block_size:
            # Fast path.
            self._hash.update(self._zero_block_digest)
        else:
            # Slow path.
            block_digest = self._func(b"\0" * count).digest()
            self._hash.update(block_digest)

    def digest(self):
        return self._hash.digest()

    def hexdigest(self):
        return self._hash.hexdigest()


def checksum(path, block_size=BLOCK_SIZE, algorithm=ALGORITHM,
             digest_size=DIGEST_SIZE, detect_zeroes=True):
    """
    Compute file checksum without extents information.

    Arguments:
        path (str): Path to image.
        block_size (int): Size of block in bytes. Should be multiple of 4096.
        algorithm (str): One of the algorithms supported py haslib module.
        digest_size (int): Size of hash in bytes, supported only for blake2b
            and blake2s algorithms; specify None for other algorithms.
        detect_zeroes (bool): If True, detect zeroes in the input, speeing up
            the calculation.
    """
    length = os.path.getsize(path)
    block = bytearray(block_size)
    h = Hash(
        block_size=block_size, algorithm=algorithm, digest_size=digest_size)

    with open(path, "rb") as f:
        # Hash full blocks.
        while length >= block_size:
            _read_block(f, block_size, block)
            if detect_zeroes and ioutil.is_zero(block):
                h.zero(block_size)
            else:
                h.update(block)
            length -= block_size

        # Hash last partial block.
        if length:
            with memoryview(block)[:length] as view:
                _read_block(f, length, view)
                h.update(view)

    return {
        "algorithm": algorithm,
        "block_size": block_size,
        "checksum": h.hexdigest(),
    }


def _read_block(f, length, buf):
    pos = 0
    while pos < length:
        with memoryview(buf)[pos:] as view:
            pos += f.readinto(view)


class Block:
    """
    Block descriptor.
    """

    __slots__ = ("start", "length", "zero")

    def __init__(self, start, length, zero):
        self.start = start
        self.length = length
        self.zero = zero

    def merge(self, other, block_size):
        """
        Merge part of another block into this block, possibly converting this
        block to a data block.
        """
        stolen = min(block_size - self.length, other.length)

        # Steal range from other...
        other.start += stolen
        other.length -= stolen

        # And add to myself, possibly converting to data block.
        self.length += stolen
        self.zero &= other.zero

    def split(self, block_size):
        """
        Split another block from this block. Valid only if this block length is
        bigger than block_size.
        """
        assert self.length >= block_size

        block = Block(self.start, block_size, self.zero)
        self.start += block_size
        self.length -= block_size

        return block

    def __repr__(self):
        return (f"Block(start={self.start}, length={self.length}, "
                f"zero={self.zero})")


def split(extents, block_size=BLOCK_SIZE):
    """
    Generate stream of block_size blocks from extents stream.

    Extents smaller than block_size will be merge into a single block of
    block_size length. Merging blocks will convert small zero block to data
    blocks, or steal part of zero block into a data block.

    Extents:  |   data   | zero |  data  |          zero                  |
    Blocks:   |     data     |     data     |     zero     |     zero     |

    Extents larger than block_size are split into multiple block_size length
    blocks.

    Extents:  |             data            |             zero            |
    Blocks:   |     data     |     data     |     zero     |     zero     |

    If the image is not aligned to block_size, the last block length will be
    smaller than block_size.

    Extents:  |     data     |             zero            | data |
    Blocks:   |     data     |     zero     |     zero     | data |
    """
    partial = None

    for extent in extents:
        current = Block(extent.start, extent.length, extent.zero)

        # Try to complete and yield partial block.
        if partial:
            partial.merge(current, block_size)
            if partial.length < block_size:
                continue

            yield partial
            partial = None

        # Yield complete blocks.
        while current.length >= block_size:
            yield current.split(block_size)

        # Keep the partial block for the next extent.
        if current.length:
            partial = current

    if partial:
        yield partial
