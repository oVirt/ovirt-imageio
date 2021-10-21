# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import functools
import logging
import os
import re
import subprocess

from contextlib import contextmanager

from ovirt_imageio._internal import sockutil

QEMU = os.environ.get("QEMU", "qemu-kvm")

log = logging.getLogger("qemu")


def supports_audiodev():
    if not hasattr(supports_audiodev, "result"):
        cmd = [QEMU, "--help"]
        out = subprocess.check_output(cmd, env=env()).decode()
        m = re.search(r"^-audiodev +none\b", out, flags=re.MULTILINE)
        supports_audiodev.result = m is not None
    return supports_audiodev.result


def env():
    """
    Amend PATH to locate qemu-kvm on platforms that hide it in /usr/libexec
    (e.g RHEL).
    """
    env = dict(os.environ)
    env["PATH"] = ":".join((env["PATH"], "/usr/libexec"))
    return env


@contextmanager
def run(image, fmt, qmp_sock=None, start_cpu=True, shutdown_timeout=1):
    # NOTES:
    # - Let qemu pick default memory size, since on some platforms memory have
    #   strange alignment. Here is a failure from ppc64le host:
    #       qemu-kvm: Memory size 0x1000000 is not aligned to 256 MiB
    cmd = [
        QEMU,
        # Use kvm if available, othrewise fallback to tcg. This allows running
        # qemu on Travis CI which does not support nested virtualization.
        "-nodefaults",
        "-machine", "accel=kvm:tcg",
        "-drive",
        "if=virtio,id=drive0,node-name=file0,file={},format={}".format(
            image, fmt),
        "-nographic",
        "-net", "none",
        "-monitor", "none",
        "-serial", "stdio",
    ]

    if qmp_sock:
        cmd.append("-qmp")
        cmd.append("unix:{},server,nowait".format(qmp_sock))

    # Workaround for bug in qemu-4.0.0-rc0 on Fedora, failing to start VM
    # becuase initilizing real audio driver failed.
    # See https://bugzilla.redhat.com/1692047.
    if supports_audiodev():
        cmd.append("-audiodev")
        cmd.append("none,id=1")

    if not start_cpu:
        cmd.append("-S")

    log.info("Starting qemu %s", cmd)
    p = subprocess.Popen(
        cmd,
        env=env(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE)
    try:
        if qmp_sock and not sockutil.wait_for_socket(qmp_sock, 1):
            raise RuntimeError("Timeout waiting for socket: %s" % qmp_sock)
        yield Guest(p)
    finally:
        log.info("Terminating qemu gracefully")
        p.terminate()
        try:
            p.wait(shutdown_timeout)
        except subprocess.TimeoutExpired:
            log.warning("Timeout terminating qemu - killing it")
            p.kill()
            p.wait()
        log.debug("qemu terminated with exit code %s", p.returncode)


class Guest:

    def __init__(self, p):
        self._stdin = p.stdin
        self._stdout = p.stdout
        self._logged = False

    def login(self, name, password):
        log.info("Logging in to guest")
        assert not self._logged
        self._wait_for("login: ")
        self._send(name)
        self._wait_for("Password: ")
        self._send(password)
        self._wait_for("# ")
        self._logged = True

    def run(self, command):
        log.info("Running command in guest: %s", command)
        self._send(command)
        return self._wait_for("# ")

    def _send(self, s):
        log.debug("Sending: %r", s)
        self._stdin.write(s.encode("utf-8") + b"\n")
        self._stdin.flush()
        self._wait_for(s + "\r\n")

    def _wait_for(self, s):
        log.debug("Waiting for: %r", s)
        data = s.encode("utf-8")
        buf = bytearray()
        while True:
            buf += self._stdout.read(1)
            if buf.endswith(data):
                rep = buf[:-len(data)]
                return rep.decode("utf-8")


@functools.lru_cache(maxsize=1)
def version():
    # Typical output:
    # QEMU emulator version 6.0.0 (qemu-6.0.0-1.fc32)
    # Copyright (c) 2003-2021 Fabrice Bellard and the QEMU Project developers
    out = subprocess.check_output([QEMU, "--version"], env=env())
    first_line = out.decode("utf-8").splitlines()[0]
    _, _, _, version_string, pkg = first_line.split(None, 4)
    return tuple(int(n) for n in version_string.split("."))
