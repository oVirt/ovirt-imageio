# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import functools
import json
import logging
import os
import signal
import stat
import subprocess
import urllib.parse

from contextlib import contextmanager

from . import nbd
from . import sockutil

log = logging.getLogger("qemu_nbd")

# Allow using non system qemu-nbd, for example built from source.
QEMU_NBD = os.environ.get("QEMU_NBD", "qemu-nbd")


class Server:

    def __init__(
            self, image, fmt, sock, export_name="", read_only=False, shared=8,
            cache=None, aio=None, discard="unmap", detect_zeroes="unmap",
            bitmap=None, backing_chain=True, offset=None, size=None,
            timeout=10.0, block_signals=None):
        """
        Initialize qemu-nbd Server.

        Arguments:
            image (str): filename to open
            fmt (str): image format (raw, qcow2, ...)
            sock (nbd.Address): socket address to listen to
            export_name (str): expose export by name
            read_only (bool): export is read-only
            shared (int): export can be shared by specified number of clients
            cache (str): cache mode (none, writeback, ...). If not specified,
                probe image and use "none" if direct I/O is supported, and
                "writeback" if not.
            aio (str): AIO mode (native or threads). If not specified, use
                "native" if cache is "none" and "threads" otherwise.
            discard (str): discard mode (ignore, unmap)
            detect_zeroes (str): Control the automatic conversion of plain zero
                writes by the OS to driver-specific optimized zero write
                commands.  Value is one of "off", "on", or "unmap".  "unmap"
                converts a zero write to an unmap operation and can only be
                used if "discard" is set to "unmap".  The default is "unmap".
            bitmap (str): export this dirty bitmap
            backing_chain (bool): when using qcow2 format, open the backing
                chain. When set to False, override the backing chain to null.
                Unallocated extents will be read as zeroes, instead of reading
                data from the backing chain. It is possible to tell if an
                extent is allocated using the extent NBD_STATE_HOLE bit.
                Using backing_chain=False requires qemu-nbd >= 5.2.0.
            offset (int): Expose a range starting at offset in raw image.
                See BlockdevOptionsRaw type in qemu source.
            size (int): Expose a range of size bytes in a raw image.
                See BlockdevOptionsRaw type in qemu source.
            block_signals (Set[int]): Signals to block in the child process.

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
        self.detect_zeroes = detect_zeroes
        self.bitmap = bitmap
        self.backing_chain = backing_chain
        self.offset = offset
        self.size = size
        self.timeout = timeout
        self.block_signals = block_signals
        self.proc = None

    @property
    def url(self):
        url = self.sock.url(self.export_name)
        return urllib.parse.urlparse(url)

    def start(self):
        # Implement "auto" cache mode, planned for qemu 7.
        if self.cache is None:
            self.cache = "none" if self._can_use_direct_io() else "writeback"
            log.debug("Using cache=%r", self.cache)

        if self.aio is None:
            self.aio = "native" if self.cache == "none" else "threads"
            log.debug("Using aio=%r", self.aio)

        cmd = [
            QEMU_NBD,
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

        if self.detect_zeroes:
            cmd.append("--detect-zeroes={}".format(self.detect_zeroes)),

        if self.bitmap:
            cmd.append("--bitmap={}".format(self.bitmap))

        # Always using --allocation-depth simplfy everything, but we want to
        # support RHEL users that have qemu-nbd 4.2.0. Add the option only on
        # qemu-nbd >= 5.2.0, and disable backing_chain=False otherwise.
        if version() >= (5, 2, 0):
            # qemu-nbd 6.2.0 introduced a regression, returning wrong
            # base:allocation results on the second call when accessing raw
            # image.  Since raw image always reports single allocation depth
            # extent, we can safely disable it for raw images.
            # https://lists.nongnu.org/archive/html/qemu-block/2022-01/msg00292.html
            if self.fmt != "raw":
                cmd.append("--allocation-depth")
        elif self.fmt == "qcow2" and not self.backing_chain:
            raise RuntimeError(
                "backing_chain=False requires qemu-nbd >= 5.2.0")

        # Build a 'json:{...}' filename allowing control all aspects of the
        # image.

        driver = "host_device" if self._is_block_device() else "file"
        file = {"driver": driver, "filename": self.image}

        if self.offset is not None or self.size is not None:
            # Exposing a range in a raw file.
            image = {"driver": "raw", "file": file}
            if self.offset is not None:
                image["offset"] = self.offset
            if self.size is not None:
                image["size"] = self.size

            if self.fmt == "qcow2":
                # Add a qcow2 driver on top of the raw driver. In this case we
                # cannot have any backing file so backing_chain is ignored.
                image = {"driver": "qcow2", "file": image}
        else:
            # Exposing the entire image using raw or qcow2 format.
            image = {"driver": self.fmt, "file": file}

            if self.fmt == "qcow2" and not self.backing_chain:
                image["backing"] = None

        cmd.append("json:" + json.dumps(image))

        log.debug("Starting qemu-nbd %s", cmd)
        self.proc = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            preexec_fn=self._preexec_fn if self.block_signals else None)

        if not sockutil.wait_for_socket(self.sock, self.timeout):
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

    def send_signal(self, signo):
        """
        Send signal to qemu-nbd.

        Must be called only when the server is running.
        """
        if self.proc is None:
            raise RuntimeError("Server not running")
        self.proc.send_signal(signo)

    def wait(self, timeout=None):
        """
        Wait timeout seconds for qemu-nbd termination and return the exit code
        or None if qemu-nbd is still running when the timeout expires.

        Must be called only when the server is running.
        """
        if self.proc is None:
            raise RuntimeError("Server not running")

        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            return None

    def _can_use_direct_io(self):
        flags = os.O_RDONLY if self.read_only else os.O_RDWR
        flags |= os.O_DIRECT
        try:
            fd = os.open(self.image, flags)
        except OSError:
            return False
        else:
            os.close(fd)
            return True

    def _is_block_device(self):
        return stat.S_ISBLK(os.stat(self.image).st_mode)

    def _preexec_fn(self):
        """
        Called in the child process just before the child is executed.
        """
        if self.block_signals:
            signal.pthread_sigmask(signal.SIG_BLOCK, self.block_signals)


@contextmanager
def run(image, fmt, sock, export_name="", read_only=False, shared=8,
        cache=None, aio=None, discard="unmap", detect_zeroes="unmap",
        bitmap=None, backing_chain=True, offset=None, size=None, timeout=10.0,
        block_signals=None):
    server = Server(
        image, fmt, sock,
        export_name=export_name,
        read_only=read_only,
        shared=shared,
        cache=cache,
        aio=aio,
        discard=discard,
        detect_zeroes=detect_zeroes,
        bitmap=bitmap,
        backing_chain=backing_chain,
        offset=offset,
        size=size,
        timeout=timeout,
        block_signals=block_signals)
    server.start()
    try:
        yield server
    finally:
        server.stop()


@contextmanager
def open(image, fmt, read_only=False, bitmap=None, discard="unmap",
         detect_zeroes="unmap", backing_chain=True, offset=None, size=None):
    """
    Open nbd client for accessing image using qemu-nbd.
    """
    sock = nbd.UnixAddress(image + ".sock")
    with run(
            image, fmt, sock,
            read_only=read_only,
            bitmap=bitmap,
            discard=discard,
            detect_zeroes=detect_zeroes,
            backing_chain=backing_chain,
            offset=offset,
            size=size):
        with nbd.Client(sock, dirty=bitmap is not None) as c:
            yield c


@functools.lru_cache(maxsize=1)
def version():
    """
    Return current version tuple (major, minir, patch).
    """
    # Typical output:
    # qemu-nbd 5.1.0 (qemu-kvm-5.1.0-20.el8)
    # ...
    out = subprocess.check_output([QEMU_NBD, "--version"])
    first_line = out.decode("utf-8").splitlines()[0]
    version_string = first_line.split(None, 2)[1]
    return tuple(int(n) for n in version_string.split("."))
