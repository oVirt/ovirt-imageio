# ovirt-imageio
# Copyright (C) 2015-2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import os

from . import checksum
from . import errors
from . import extents
from . import http
from . import images
from . import info
from . import profile
from . import ssl
from . import stats
from . import tickets
from . import uhttp
from . import util

DEFAULT_SOCKET_MODE = 0o660

log = logging.getLogger("services")


class Service:

    name = None

    def start(self):
        log.debug("Starting %s", self.name)
        util.start_thread(self._run, name=self.name)

    def stop(self):
        log.debug("Stopping %s", self.name)
        self._server.shutdown()

    @property
    def port(self):
        return self._server.server_port

    @property
    def address(self):
        return self._server.server_address

    def _run(self):
        log.debug("%s started", self.name)
        self._server.serve_forever(
            poll_interval=self._config.daemon.poll_interval)
        log.debug("%s terminated normally", self.name)


class RemoteService(Service):
    """
    Service used to access images data from remote host.

    Access to this service requires a valid ticket that can be installed using
    the local control service.
    """

    name = "remote.service"

    def __init__(self, config, auth):
        self._config = config
        port = config.remote.port
        if not 0 <= port < 0xFFFF:
            raise errors.InvalidConfig("remote.port", port)
        log.debug("Creating %s on port %d", self.name, port)
        self._server = http.Server((config.remote.host, port), http.Connection)
        # TODO: Make clock configurable, disabled by default.
        self._server.clock_class = stats.Clock
        if port == 0:
            config.remote.port = self.port
        if config.tls.enable:
            self._secure_server()
        self._server.app = http.Router([
            (r"/images/(.*)/extents", extents.Handler(config, auth)),
            (r"/images/(.*)/checksum/algorithms",
                checksum.Algorithms(config, auth)),
            (r"/images/(.*)/checksum", checksum.Checksum(config, auth)),
            (r"/images/(.*)", images.Handler(config, auth)),
            (r"/info/", info.Handler(config, auth)),
        ])
        log.info("%s listening on %r", self.name, self.address)

    def _secure_server(self):
        if "" in (self._config.tls.cert_file, self._config.tls.key_file):
            raise errors.TlsConfigurationError(self._config.tls)

        log.debug("Securing server (cafile=%s, certfile=%s, keyfile=%s)",
                  self._config.tls.ca_file,
                  self._config.tls.cert_file,
                  self._config.tls.key_file)
        context = ssl.server_context(
            self._config.tls.cert_file,
            self._config.tls.key_file,
            cafile=self._config.tls.ca_file,
            enable_tls1_1=self._config.tls.enable_tls1_1)
        self._server.socket = context.wrap_socket(
            self._server.socket, server_side=True)


class LocalService(Service):
    """
    Service used to access images locally.

    Access to this service requires a valid ticket that can be installed using
    the control service.
    """

    name = "local.service"

    def __init__(self, config, auth):
        self._config = config
        log.debug("Creating %s on socket %r", self.name, config.local.socket)
        self._server = uhttp.Server(config.local.socket, uhttp.Connection)
        # TODO: Make clock configurable, disabled by default.
        self._server.clock_class = stats.Clock
        if config.local.socket == "":
            config.local.socket = self.address
        self._server.app = http.Router([
            (r"/images/(.*)/extents", extents.Handler(config, auth)),
            (r"/images/(.*)/checksum/algorithms",
                checksum.Algorithms(config, auth)),
            (r"/images/(.*)/checksum", checksum.Checksum(config, auth)),
            (r"/images/(.*)", images.Handler(config, auth)),
        ])
        log.info("%s listening on %r", self.name, self.address)


class ControlService(Service):
    """
    Service used to control imageio daemon on a host.

    The service is using unix socket owned by a program managing the host. Only
    this program can access the socket.
    """

    name = "control.service"

    def __init__(self, config, auth):
        self._config = config
        transport = self._config.control.transport.lower()
        if transport == "tcp":
            port = config.control.port
            log.debug("Creating %s on port %r", self.name, port)
            if not 0 <= port < 0xFFFF:
                raise errors.InvalidConfig("control.port", port)
            self._server = http.Server(
                ("localhost", port),
                http.Connection,
                prefer_ipv4=config.control.prefer_ipv4)
            if port == 0:
                config.control.port = self.port
        elif transport == "unix":
            socket = config.control.socket
            log.debug("Creating %s on socket %r", self.name, socket)
            self._server = uhttp.Server(socket, uhttp.Connection)
            if socket == "":
                config.control.socket = self.address
            os.chmod(config.control.socket, DEFAULT_SOCKET_MODE)
        else:
            raise errors.InvalidConfig("control.transport", transport)

        # TODO: Make clock configurable, disabled by default.
        self._server.clock_class = stats.Clock

        self._server.app = http.Router([
            (r"/tickets/(.*)", tickets.Handler(config, auth)),
            (r"/profile/", profile.Handler(config, auth)),
        ])
        log.info("%s listening on %r", self.name, self.address)
