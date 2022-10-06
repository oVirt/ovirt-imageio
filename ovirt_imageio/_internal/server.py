# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

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

from . import auth
from . import config
from . import errors
from . import services
from . import version

DEFAULT_CONF_DIR = "/etc/ovirt-imageio"
VENDOR_CONF_DIR = "/usr/lib/ovirt-imageio"

log = logging.getLogger("server")


def main():
    args = parse_args()
    try:
        cfg = load_config(args.conf_dir)
        if args.show_config:
            show_config(cfg)
            return

        configure_logger(cfg)
        log.info("Starting (hostname=%s pid=%s, version=%s)",
                 socket.gethostname(), os.getpid(), version.string)

        server = Server(cfg, ticket=args.ticket)
        signal.signal(signal.SIGINT, server.terminate)
        signal.signal(signal.SIGTERM, server.terminate)

        server.start()
        try:
            notify_systemd(cfg)
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
        default=DEFAULT_CONF_DIR,
        help="path to configuration directory, where daemon.conf and "
             "logger.conf are located.")
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="print actual configuration in json format and exit. This is "
             "useful for debugging configuration issues, or reading imageio "
             "configuration by other programs.")
    parser.add_argument(
        "-t", "--ticket",
        help="path to a ticket to load during server startup.")
    return parser.parse_args()


def show_config(cfg):
    print(json.dumps(config.to_dict(cfg), indent=4))


def load_config(conf_dir):
    pattern = os.path.join(conf_dir, "conf.d", "*.conf")
    files = glob.glob(pattern)

    # Trying to use invalid configuration directory is a user error or
    # broken installation. Failing fast will help to fix the issue.
    # https://github.com/oVirt/ovirt-imageio/issues/33
    if not files:
        raise ValueError(f"Could not find {pattern}")

    # Vendor may override application defaults if needed.
    pattern = os.path.join(VENDOR_CONF_DIR, "conf.d", "*.conf")
    files.extend(glob.glob(pattern))

    # Override files based on file name sort order:
    # - /var/lib/ovirt-imageio/conf.d/75-vendor.conf overrides
    #   /etc/ovirt-imageio/conf.d/50-vdsm.conf.
    # - /etc/ovirt-imageio/conf.d/99-user.conf overrides
    #   /var/lib/ovirt-imageio/conf.d/75-vendor.conf.
    files.sort(key=os.path.basename)

    return config.load(files)


def configure_logger(cfg):
    parser = configparser.RawConfigParser()
    parser.read_dict(config.to_dict(cfg))
    logging.config.fileConfig(parser, disable_existing_loggers=False)


def notify_systemd(cfg):
    if cfg.daemon.systemd_enable:
        log.debug("Notifying systemd")
        import systemd.daemon
        systemd.daemon.notify("READY=1")


class Server:

    def __init__(self, config, ticket=None):
        self.config = config
        self.running = False
        self.auth = auth.Authorizer(config)
        self.remote_service = services.RemoteService(self.config, self.auth)

        self.local_service = None
        if config.local.enable:
            self.local_service = services.LocalService(self.config, self.auth)

        self.control_service = None
        if config.control.enable:
            self.control_service = services.ControlService(
                self.config, self.auth)

        if os.geteuid() == 0 and self.config.daemon.drop_privileges:
            self._drop_privileges()

        if ticket:
            self._add_ticket(ticket)

    def start(self):
        assert not self.running
        self.running = True

        self.remote_service.start()
        if self.local_service is not None:
            self.local_service.start()
        if self.control_service is not None:
            self.control_service.start()

    def stop(self):
        log.debug("Stopping services")
        self.remote_service.stop()
        if self.local_service is not None:
            self.local_service.stop()
        if self.control_service is not None:
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

        # Restore ownership of control socket if used.
        if self.control_service is not None:
            transport = self.config.control.transport.lower()
            if transport == "unix":
                os.chown(self.config.control.socket, uid, gid)

        # Set new uid and gid for the process.
        log.debug("Dropping root privileges, running as %i:%i" % (uid, gid))
        os.initgroups(self.config.daemon.user_name, gid)
        os.setgid(gid)
        os.setuid(uid)

    def _add_ticket(self, ticket):
        log.info("Initial ticket: %s", ticket)
        self.auth.add(self._read_ticket(ticket))

    def _read_ticket(self, ticket):
        try:
            with open(ticket, 'r') as f:
                return json.loads(f.read())
        except ValueError as e:
            raise errors.InvalidTicket(
                f"Cannot parse ticket {ticket}: {e}") from e
        except FileNotFoundError as e:
            raise errors.InvalidTicket(f"Cannot read ticket: {e}") from e
