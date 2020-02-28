# ovirt-imageio-daemon
# Copyright (C) 2015-2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import argparse
import logging
import logging.config
import os
import signal
import sys

import systemd.daemon

from . import auth
from . import config
from . import services
from . import version

log = logging.getLogger("server")


def main():
    args = parse_args()
    configure_logger(args)
    try:
        log.info("Starting (pid=%s, version=%s)", os.getpid(), version.string)
        cfg = config.load([os.path.join(args.conf_dir, "daemon.conf")])

        server = Server(cfg)
        signal.signal(signal.SIGINT, server.terminate)
        signal.signal(signal.SIGTERM, server.terminate)

        server.start()
        try:
            systemd.daemon.notify("READY=1")
            log.info("Ready for requests")
            while server.running:
                signal.pause()
        finally:
            server.stop()
        log.info("Stopped")
    except Exception:
        log.exception("Server failed")
        sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--conf-dir",
        default="/etc/ovirt-imageio",
        help="path to configuration directory, where daemon.conf and "
             "logger.conf are located.")
    return parser.parse_args()


def configure_logger(args):
    conf = os.path.join(args.conf_dir, "logger.conf")
    logging.config.fileConfig(conf, disable_existing_loggers=False)


class Server:

    def __init__(self, config):
        self.config = config
        self.auth = auth.Authorizer()
        self.remote_service = None
        self.local_service = None
        self.control_service = None
        self.running = False

    def start(self):
        assert not self.running
        self.running = True

        log.debug("Starting remote service on port %d",
                  self.config.images.port)
        self.remote_service = services.RemoteService(self.config, self.auth)
        self.remote_service.start()

        log.debug("Starting local service on socket %r",
                  self.config.images.socket)
        self.local_service = services.LocalService(self.config, self.auth)
        self.local_service.start()

        log.debug("Starting control service on socket %r",
                  self.config.tickets.socket)
        self.control_service = services.ControlService(self.config, self.auth)
        self.control_service.start()

    def stop(self):
        log.debug("Stopping services")
        self.remote_service.stop()
        self.local_service.stop()
        self.control_service.stop()
        self.remote_service = None
        self.local_service = None
        self.control_service = None

    def terminate(self, signo, frame):
        log.info("Received signal %d, shutting down", signo)
        self.running = False
