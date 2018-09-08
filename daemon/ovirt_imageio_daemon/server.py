# ovirt-imageio-daemon
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import logging
import logging.config
import os
import signal
import sys
import time

import systemd.daemon

from ovirt_imageio_common import configloader
from ovirt_imageio_common import ssl
from ovirt_imageio_common import util
from ovirt_imageio_common import version
from ovirt_imageio_common import web

from . import config
from . import images
from . import pki
from . import profile
from . import tickets
from . import uhttp
from . import wsgi

CONF_DIR = "/etc/ovirt-imageio-daemon"

log = logging.getLogger("server")
remote_service = None
local_service = None
control_service = None
running = True


def main(args):
    configure_logger()
    try:
        log.info("Starting (pid=%s, version=%s)", os.getpid(), version.string)
        configloader.load(config, [os.path.join(CONF_DIR, "daemon.conf")])
        signal.signal(signal.SIGINT, terminate)
        signal.signal(signal.SIGTERM, terminate)
        start(config)
        try:
            systemd.daemon.notify("READY=1")
            log.info("Ready for requests")
            while running:
                time.sleep(30)
        finally:
            stop()
        log.info("Stopped")
    except Exception:
        log.exception(
            "Service failed (remote_service=%s, local_service=%s, "
            "control_service=%s, running=%s)"
            % (remote_service, local_service, control_service, running))
        sys.exit(1)


def configure_logger():
    conf = os.path.join(CONF_DIR, "logger.conf")
    logging.config.fileConfig(conf, disable_existing_loggers=False)


def terminate(signo, frame):
    global running
    log.info("Received signal %d, shutting down", signo)
    running = False


def start(config):
    global remote_service, local_service, control_service
    assert not (remote_service or local_service or control_service)

    log.debug("Starting remote service on port %d", config.images.port)
    remote_service = RemoteService(config)
    remote_service.start()

    log.debug("Starting local service on socket %r", config.images.socket)
    local_service = LocalService(config)
    local_service.start()

    log.debug("Starting control service on socket %r", config.tickets.socket)
    control_service = ControlService(config)
    control_service.start()


def stop():
    global remote_service, local_service, control_service
    log.debug("Stopping services")
    remote_service.stop()
    local_service.stop()
    control_service.stop()
    remote_service = None
    local_service = None
    control_service = None


class Service(object):

    name = None

    def start(self):
        log.debug("Starting %s", self.name)
        util.start_thread(
            self._server.serve_forever,
            kwargs={"poll_interval": self._config.daemon.poll_interval},
            name=self.name)

    def stop(self):
        log.debug("Stopping %s", self.name)
        self._server.shutdown()

    @property
    def port(self):
        return self._server.server_port

    @property
    def address(self):
        return self._server.server_address


class RemoteService(Service):
    """
    Service used to access images data from remote host.

    Access to this service requires a valid ticket that can be installed using
    the local control service.
    """

    name = "remote.service"

    def __init__(self, config):
        self._config = config
        self._server = wsgi.WSGIServer(
            (config.images.host, config.images.port),
            wsgi.WSGIRequestHandler)
        if config.images.port == 0:
            config.images.port = self.port
        self._secure_server()
        app = web.Application(config, [(r"/images/(.*)", images.Handler)])
        self._server.set_app(app)
        log.debug("%s listening on port %d", self.name, self.port)

    def _secure_server(self):
        key_file = pki.key_file(self._config)
        cert_file = pki.cert_file(self._config)
        log.debug("Securing server (certfile=%s, keyfile=%s)",
                  cert_file, key_file)
        context = ssl.server_context(
            cert_file, cert_file, key_file,
            enable_tls1_1=self._config.daemon.enable_tls1_1)
        self._server.socket = context.wrap_socket(
            self._server.socket, server_side=True)


class LocalService(Service):
    """
    Service used to access images locally.

    Access to this service requires a valid ticket that can be installed using
    the control service.
    """

    name = "local.service"

    def __init__(self, config):
        self._config = config
        self._server = uhttp.UnixWSGIServer(
            config.images.socket, uhttp.UnixWSGIRequestHandler)
        if config.images.socket == "":
            config.images.socket = self.address
        app = web.Application(config, [(r"/images/(.*)", images.Handler)])
        self._server.set_app(app)
        log.debug("%s listening on %r", self.name, self.address)


class ControlService(Service):
    """
    Service used to control imageio daemon on a host.

    The service is using unix socket owned by a program managing the host. Only
    this program can access the socket.
    """

    name = "control.service"

    def __init__(self, config):
        self._config = config
        self._server = uhttp.UnixWSGIServer(
            config.tickets.socket, uhttp.UnixWSGIRequestHandler)
        if config.tickets.socket == "":
            config.tickets.socket = self.address
        app = web.Application(config, [
            (r"/tickets/(.*)", tickets.Handler),
            (r"/profile/", profile.Handler),
        ])
        self._server.set_app(app)
        log.debug("%s listening on %r", self.name, self.address)
