# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
import logging
import socket

log = logging.getLogger("qmp")


class Error(Exception):
    msg = None

    def __str__(self):
        return self.msg.format(self=self)


class CommandFailed(Error):
    msg = "Command {self.cmd} failed: {self.error}"

    def __init__(self, cmd, error):
        self.cmd = cmd
        self.error = error


class NotFound(Error):
    msg = "No node for {self.path}: {self.nodes}"

    def __init__(self, path, nodes):
        self.path = path
        self.nodes = nodes


class Client:

    def __init__(self, sock):
        self.s = socket.socket(socket.AF_UNIX)
        self.s.connect(sock)
        self.r = self.s.makefile("rb")
        self.w = self.s.makefile("wb", 0)
        self._handshake()

    def close(self):
        self.r.close()
        self.w.close()
        self.s.close()

    def execute(self, name, args=None):
        cmd = {"execute": name}
        if args:
            cmd["arguments"] = args
        self._send(cmd)
        while True:
            msg = self._recv()
            if "return" in msg:
                log.debug("Received return: %(return)s", msg)
                return msg["return"]
            elif "error" in msg:
                raise CommandFailed(cmd, msg["error"])
            elif "event" in msg:
                log.debug("Received event: %(event)s: %(data)s" % msg)
            else:
                log.warning("Received unexpected message: %s" % msg)

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        try:
            self.close()
        except Exception:
            if t is not None:
                log.exception("Error closing")

    def _handshake(self):
        msg = self._recv()
        if "QMP" not in msg:
            raise Error("Unexpected response from qemu: %s" % msg)
        log.debug("Talking with qemu %(major)s.%(minor)s.%(micro)s",
                  msg["QMP"]["version"]["qemu"])
        self.execute("qmp_capabilities")

    def _send(self, cmd):
        log.debug("Sending %s", cmd)
        msg = json.dumps(cmd).encode("utf-8")
        self.w.write(msg + b"\n")

    def _recv(self):
        msg = self.r.readline()
        return json.loads(msg.decode("utf-8"))


def find_node(c, path):
    """
    Use connected client c to find a block node using image.

    Raises NotFound if no node is using image.
    """
    r = c.execute("query-block")
    for b in r:
        if "inserted" in b and b["inserted"]["file"] == path:
            return b
    raise NotFound(path, r)
