# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import hashlib
import pytest
from functools import partial

from ovirt_imageio._internal import blkhash

default_hash = partial(hashlib.blake2b, digest_size=32)


def test_algorithm_basic():
    h = default_hash()
    for i in range(10):
        block = (b"%02d\n" % i).ljust(blkhash.BLOCK_SIZE, b"\0")
        block_digest = default_hash(block).digest()
        h.update(block_digest)
    assert h.hexdigest() == (
        "7934079f80b53142d738d2bb7efaedf696a3d34d76a7865a24130bc7b4a7acfe"
    )


def test_algorithm_zero_optimization():
    # Hash the entire image. This is the case when we don't have extent
    # information, and do not use zero detection.
    zero_block = b"\0" * blkhash.BLOCK_SIZE
    h1 = default_hash()
    for i in range(10):
        block_digest = default_hash(zero_block).digest()
        h1.update(block_digest)

    # Hash a pre-computed digest instead of the actual bytes. Here we either
    # have extent information, or we detected that the blocks are zero blocks.
    h2 = default_hash()
    block_digest = default_hash(zero_block).digest()
    for i in range(10):
        h2.update(block_digest)

    # We must get the same checksum in both cases.
    assert h1.hexdigest() == h2.hexdigest()


def test_hasher_data():
    h1 = blkhash.Hash()
    for i in range(10):
        block = (b"%02d\n" % i).ljust(blkhash.BLOCK_SIZE, b"\0")
        h1.update(block)

    h2 = default_hash()
    for i in range(10):
        block = (b"%02d\n" % i).ljust(blkhash.BLOCK_SIZE, b"\0")
        block_digest = default_hash(block).digest()
        h2.update(block_digest)

    assert h1.hexdigest() == h2.hexdigest()


def test_hasher_zero():
    block = b"\0" * blkhash.BLOCK_SIZE

    h1 = blkhash.Hash()
    h1.update(block)
    h1.update(block)

    h2 = blkhash.Hash()
    h2.zero(len(block))
    h2.zero(len(block))

    assert h1.hexdigest() == h2.hexdigest()


@pytest.mark.parametrize("size,algorithm,digest_size,checksum", [
    # Files aligned to block size.
    (4 * 1024**2, "blake2b", 32,
        "f426bb2cf1e1901fe4e87423950944ecfed6d9d18a09e6e802aa4912e1c9b2d6"),
    (4 * 1024**2, "sha1", None,
        "3ed03b375b6658d99b63ced1867a95aeef080b79"),
    # Files not aligned to block size.
    (3 * 1024**2, "blake2b", 32,
        "42f3b76772a6d3dcffae2a24697721687975e2c60ddfd4ba7831ea9ce772ca71"),
    (3 * 1024**2, "sha1", None,
        "6cba43b908381be45a55ab9b4361f8370b928354"),
    (5 * 1024**2, "blake2b", 32,
        "0da53b583fc1fbbac7edea14454c79f84a8107613e614f2c7a47071dfdcf41a6"),
    (5 * 1024**2, "sha1", None,
        "d3936edd8e3a8ff10e8257a9f460d8da67838549"),
    # Empty file.
    (0, "blake2b", 32,
        "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"),
    (0, "sha1", None,
        "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
])
def test_checksum(tmpdir, size, algorithm, digest_size, checksum):
    path = str(tmpdir.join("file"))

    with open(path, "wb") as f:
        f.write(b"data")
        f.truncate(size)

    actual = blkhash.checksum(
        path,
        block_size=blkhash.BLOCK_SIZE,
        algorithm=algorithm,
        digest_size=digest_size)

    assert actual == {
        "algorithm": algorithm,
        "block_size": blkhash.BLOCK_SIZE,
        "checksum": checksum,
    }
