# ovirt-imageio
# Copyright (C) 2015-2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import argparse
import configparser
import glob
import grp
import json
import logging
import logging.config
import os
import pwd
import signal
import socket
import sys

import systemd.daemon

from . import auth
from . import config
from . import services
from . import version

VENDOR_CONF_DIR = "/usr/lib/ovirt-imageio"

log = logging.getLogger("server")


def main():
    args = parse_args()
    try:
        cfg = load_config(args)
        if args.show_config:
            show_config(cfg)
            return

        configure_logger(cfg)
        log.info("Starting (hostname=%s pid=%s, version=%s)",
                 socket.gethostname(), os.getpid(), version.string)

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
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="print actual configuration in json format and exit. This is "
             "useful for debugging configuration issues, or reading imageio "
             "configuration by other programs.")
    return parser.parse_args()


def show_config(cfg):
    print(json.dumps(config.to_dict(cfg), indent=4))


def find_configs(cfg_dirs):
    files = []
    for path in cfg_dirs:
        pattern = os.path.join(path, "conf.d", "*.conf")
        files.extend(glob.glob(pattern))
    files.sort(key=os.path.basename)
    return files


def load_config(args):
    files = find_configs([VENDOR_CONF_DIR, args.conf_dir])
    return config.load(files)


def configure_logger(cfg):
    parser = configparser.RawConfigParser()
    parser.read_dict(config.to_dict(cfg))
    logging.config.fileConfig(parser, disable_existing_loggers=False)


class Server:

    def __init__(self, config):
        self.config = config
        self.running = False
        self.auth = auth.Authorizer(config)
        self.remote_service = services.RemoteService(self.config, self.auth)
        self.local_service = None
        if config.local.enable:
            self.local_service = services.LocalService(self.config, self.auth)
        self.control_service = services.ControlService(self.config, self.auth)

        if os.geteuid() == 0 and self.config.daemon.drop_privileges:
            self._drop_privileges()

    def start(self):
        assert not self.running
        self.running = True

        self.remote_service.start()
        if self.local_service is not None:
            self.local_service.start()
        self.control_service.start()

    def stop(self):
        log.debug("Stopping services")
        self.remote_service.stop()
        if self.local_service is not None:
            self.local_service.stop()
        self.control_service.stop()

    def terminate(self, signo, frame):
        log.info("Received signal %d, shutting down", signo)
        self.running = False

    def _drop_privileges(self):
        uid = pwd.getpwnam(self.config.daemon.user_name).pw_uid
        gid = grp.getgrnam(self.config.daemon.group_name).gr_gid

        # Restore ownership of run directory.
        run_dir = self.config.daemon.run_dir
        if os.path.exists(run_dir):
            log.debug(
                "Changing ownership of %s to %i:%i" % (run_dir, uid, gid))
            os.chown(run_dir, uid, gid)

        # Restore ownership of log files.
        for h in logging.root.handlers:
            # Support only logging.FileHandler and sub-classes.
            filename = getattr(h, "baseFilename", None)
            if filename is not None:
                log.debug(
                    "Changing ownership of %s to %i:%i" % (filename, uid, gid))
                os.chown(filename, uid, gid)

        # Restore ownership of control socket is used.
        transport = self.config.control.transport.lower()
        if transport == "unix":
            os.chown(self.config.control.socket, uid, gid)

        # Set new uid and gid for the process.
        log.debug("Dropping root privileges, running as %i:%i" % (uid, gid))
        os.initgroups(self.config.daemon.user_name, gid)
        os.setgid(gid)
        os.setuid(uid)
