# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import subprocess

from contextlib import contextmanager

from ovirt_imageio_common import nbd

from . import testutil

log = logging.getLogger("qemu_nbd")


@contextmanager
def run(image, fmt, sock, export_name="", read_only=False):
    cmd = [
        "qemu-nbd",
        "--socket", sock,
        "--format", fmt,
        "--export-name", export_name.encode("utf-8"),
        "--persistent",
        "--cache=none",
        "--aio=native",
        "--discard=unmap",
    ]

    if read_only:
        cmd.append("--read-only")

    cmd.append(image)

    log.debug("Starting qemu-nbd %s", cmd)
    p = subprocess.Popen(cmd)
    try:
        if not testutil.wait_for_path(sock, 1):
            raise RuntimeError("Timeout waiting for qemu-nbd socket")
        log.debug("qemu-nbd socket ready")
        yield
    finally:
        log.debug("Terminating qemu-nbd")
        p.terminate()
        p.wait()
        log.debug("qemu-nbd terminated with exit code %s", p.returncode)


@contextmanager
def open(image, fmt, read_only=False):
    """
    Open nbd client for accessing image using qemu-nbd.
    """
    sock = image + ".sock"
    with run(image, fmt, sock, read_only=read_only):
        with nbd.Client(sock) as c:
            yield c
