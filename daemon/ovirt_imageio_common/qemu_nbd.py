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

from six.moves import urllib_parse

from . import nbd
from . import nbdutil

log = logging.getLogger("qemu_nbd")


class Server(object):

    def __init__(
            self, image, fmt, sock, export_name="", read_only=False, shared=1,
            cache="none", aio="native", discard="unmap", timeout=10.0):
        """
        Initialize qemu-nbd Server.

        Arguments:
            image (str): filename to open
            fmt (str): image format (raw, qcow2, ...)
            sock (nbd.Address): socket address to listen to
            export_name (str): expose export by name
            read_only (bool): export is read-only
            shared (int): export can be shared by specified number of clients
            cache (str): cache mode (none, writeback, ...)
            aio (str): AIO mode (native or threads)
            discard (str): discard mode (ignore, unmap)

        See qemu-nbd(8) for more info on these options.
        """
        self.image = image
        self.fmt = fmt
        self.sock = sock
        self.export_name = export_name
        self.read_only = read_only
        self.shared = shared
        self.cache = cache
        self.aio = aio
        self.discard = discard
        self.timeout = timeout
        self.proc = None

    @property
    def url(self):
        url = self.sock.url(self.export_name)
        return urllib_parse.urlparse(url)

    def start(self):
        cmd = [
            "qemu-nbd",
            "--format={}".format(self.fmt),
            "--export-name={}".format(self.export_name),
            "--persistent",
            "--shared={}".format(self.shared),
        ]

        if self.sock.transport == "unix":
            cmd.append("--socket={}".format(self.sock.path))
        elif self.sock.transport == "tcp":
            cmd.append("--bind={}".format(self.sock.host))
            cmd.append("--port={}".format(self.sock.port))
        else:
            raise RuntimeError("Unsupported transport: {}".format(self.sock))

        if self.read_only:
            cmd.append("--read-only")

        if self.cache:
            cmd.append("--cache={}".format(self.cache))

        if self.aio:
            cmd.append("--aio={}".format(self.aio))

        if self.discard:
            cmd.append("--discard={}".format(self.discard)),

        cmd.append(self.image)

        log.debug("Starting qemu-nbd %s", cmd)
        self.proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)

        if not nbdutil.wait_for_socket(self.sock, self.timeout):
            self.stop()
            raise RuntimeError("Timeout waiting for qemu-nbd socket")

        log.debug("qemu-nbd socket ready")

    def stop(self):
        if self.proc:
            log.debug("Terminating qemu-nbd gracefully")
            self.proc.terminate()

            try:
                _, err = self.proc.communicate(self.timeout)
            except subprocess.TimeoutExpired:
                log.warning("Timeout terminating qemu-nbd - killing it")
                self.proc.kill()
                _, err = self.proc.communicate(self.timeout)

            if self.proc.returncode == 0:
                log.debug("qemu-nbd terminated normally err=%r", err)
            else:
                log.warning("qemu-nbd failed rc=%s err=%r",
                            self.proc.returncode, err)

            self.proc = None


@contextmanager
def run(image, fmt, sock, export_name="", read_only=False, shared=1,
        cache="none", aio="native", discard="unmap", timeout=10.0):
    server = Server(
        image, fmt, sock,
        export_name=export_name,
        read_only=read_only,
        shared=shared,
        cache=cache,
        aio=aio,
        discard=discard,
        timeout=timeout)
    server.start()
    try:
        yield
    finally:
        server.stop()


@contextmanager
def open(image, fmt, read_only=False):
    """
    Open nbd client for accessing image using qemu-nbd.
    """
    sock = nbd.UnixAddress(image + ".sock")
    with run(image, fmt, sock, read_only=read_only):
        with nbd.Client(sock) as c:
            yield c
