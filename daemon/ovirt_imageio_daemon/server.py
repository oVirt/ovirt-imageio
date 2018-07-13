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
import webob

from webob.exc import (
    HTTPBadRequest,
    HTTPForbidden,
    HTTPNotFound,
)

from ovirt_imageio_common import configloader
from ovirt_imageio_common import directio
from ovirt_imageio_common import errors
from ovirt_imageio_common import ssl
from ovirt_imageio_common import util
from ovirt_imageio_common import version
from ovirt_imageio_common import validate
from ovirt_imageio_common import web

from . import config
from . import pki
from . import profile
from . import uhttp
from . import tickets
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
        app = web.Application(config, [(r"/images/(.*)", Images)])
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
        app = web.Application(config, [(r"/images/(.*)", Images)])
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
            (r"/tickets/(.*)", Tickets),
            (r"/profile/", profile.Handler),
        ])
        self._server.set_app(app)
        log.debug("%s listening on %r", self.name, self.address)


class Images(object):
    """
    Request handler for the /images/ resource.
    """
    log = logging.getLogger("images")

    def __init__(self, config, request, clock=None):
        self.config = config
        self.request = request
        self.clock = clock

    def put(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        size = self.request.content_length
        if size is None:
            raise HTTPBadRequest("Content-Length header is required")
        if size < 0:
            raise HTTPBadRequest("Invalid Content-Length header: %r" % size)
        content_range = web.content_range(self.request)
        offset = content_range.start or 0

        # For backward compatibility, we flush by default.
        flush = validate.enum(self.request.params, "flush", ("y", "n"),
                              default="y")
        flush = (flush == "y")

        ticket = tickets.authorize(ticket_id, "write", offset, size)
        # TODO: cancel copy if ticket expired or revoked
        self.log.info(
            "WRITE size=%d offset=%d flush=%s path=%s ticket=%s",
            size, offset, flush, ticket.url.path, ticket_id)
        op = directio.Receive(
            ticket.url.path,
            self.request.body_file_raw,
            size,
            offset=offset,
            flush=flush,
            buffersize=self.config.daemon.buffer_size,
            clock=self.clock)
        try:
            ticket.run(op)
        except errors.PartialContent as e:
            raise HTTPBadRequest(str(e))
        return web.response()

    def get(self, ticket_id):
        # TODO: cancel copy if ticket expired or revoked
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        # TODO: support partial range (e.g. bytes=0-*)

        offset = 0
        size = None
        if self.request.range:
            offset = self.request.range.start
            if self.request.range.end is not None:
                size = self.request.range.end - offset

        ticket = tickets.authorize(ticket_id, "read", offset, size)
        if size is None:
            size = ticket.size - offset
        self.log.info("READ size=%d offset=%d path=%s ticket=%s",
                      size, offset, ticket.url.path, ticket_id)
        op = directio.Send(
            ticket.url.path,
            None,
            size,
            offset=offset,
            buffersize=self.config.daemon.buffer_size,
            clock=self.clock)
        content_disposition = "attachment"
        if ticket.filename:
            filename = ticket.filename.encode("utf-8")
            content_disposition += "; filename=%s" % filename
        resp = webob.Response(
            status=206 if self.request.range else 200,
            app_iter=ticket.bind(op),
            content_type="application/octet-stream",
            content_length=str(size),
            content_disposition=content_disposition,
        )
        if self.request.range:
            content_range = self.request.range.content_range(ticket.size)
            resp.headers["content-range"] = str(content_range)

        return resp

    def patch(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            msg = self.request.json
        except ValueError as e:
            raise HTTPBadRequest("Invalid JSON message: %s" % e)

        op = validate.enum(msg, "op", ("zero", "flush"))
        if op == "zero":
            return self._zero(ticket_id, msg)
        elif op == "flush":
            return self._flush(ticket_id, msg)
        else:
            raise RuntimeError("Unreachable")

    def _zero(self, ticket_id, msg):
        size = validate.integer(msg, "size", minval=0)
        offset = validate.integer(msg, "offset", minval=0, default=0)
        flush = validate.boolean(msg, "flush", default=False)

        ticket = tickets.authorize(ticket_id, "write", offset, size)

        self.log.info(
            "ZERO size=%d offset=%d flush=%s path=%s ticket=%s",
            size, offset, flush, ticket.url.path, ticket_id)
        op = directio.Zero(
            ticket.url.path,
            size,
            offset=offset,
            flush=flush,
            buffersize=self.config.daemon.buffer_size,
            clock=self.clock)
        try:
            ticket.run(op)
        except errors.PartialContent as e:
            raise HTTPBadRequest(str(e))
        return web.response()

    def _flush(self, ticket_id, msg):
        ticket = tickets.authorize(ticket_id, "write", 0, 0)
        self.log.info("FLUSH path=%s ticket=%s", ticket.url.path, ticket_id)
        op = directio.Flush(ticket.url.path, clock=self.clock)
        ticket.run(op)
        return web.response()

    def options(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")

        if ticket_id == "*":
            # Reporting the meta-capabilities for all images.
            allow = ["OPTIONS", "GET", "PUT", "PATCH"]
            features = ["zero", "flush"]
        else:
            # Reporting real image capabilities per ticket.
            try:
                ticket = tickets.get(ticket_id)
            except KeyError:
                raise HTTPForbidden("No such ticket %r" % ticket_id)

            # Accessing ticket options considered as client activity.
            ticket.touch()

            allow = ["OPTIONS"]
            features = []
            if ticket.may("read"):
                allow.append("GET")
            if ticket.may("write"):
                allow.extend(("PUT", "PATCH"))
                features = ["zero", "flush"]

        return web.response(
            payload={
                "features": features,
                "unix_socket": self.config.images.socket,
            },
            allow=",".join(allow))


class Tickets(object):
    """
    Request handler for the /tickets/ resource.
    """
    log = logging.getLogger("tickets")

    def __init__(self, config, request, clock=None):
        self.config = config
        self.request = request
        self.clock = clock

    def get(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            ticket = tickets.get(ticket_id)
        except KeyError:
            raise HTTPNotFound("No such ticket %r" % ticket_id)
        ticket_info = ticket.info()
        self.log.debug("GET ticket=%s", ticket_info)
        return web.response(payload=ticket_info)

    def put(self, ticket_id):
        # TODO
        # - reject invalid or expired ticket
        # - start expire timer
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")

        try:
            ticket_dict = self.request.json
        except ValueError as e:
            raise HTTPBadRequest("Ticket is not in a json format: %s" % e)

        try:
            tickets.add(ticket_dict)
        except errors.InvalidTicket as e:
            raise HTTPBadRequest("Invalid ticket: %s" % e)

        return web.response()

    def patch(self, ticket_id):
        # TODO: restart expire timer
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            patch = self.request.json
        except ValueError as e:
            raise HTTPBadRequest("Invalid patch: %s" % e)
        try:
            timeout = patch["timeout"]
        except KeyError:
            raise HTTPBadRequest("Missing timeout key")
        try:
            timeout = int(timeout)
        except ValueError as e:
            raise HTTPBadRequest("Invalid timeout value: %s" % e)
        try:
            ticket = tickets.get(ticket_id)
        except KeyError:
            raise HTTPNotFound("No such ticket: %s" % ticket_id)
        ticket.extend(timeout)
        return web.response()

    def delete(self, ticket_id):
        """
        Delete a ticket if exists.

        Note that DELETE is idempotent;  the client can issue multiple DELETE
        requests in case of network failures. See
        https://tools.ietf.org/html/rfc7231#section-4.2.2.
        """
        # TODO: cancel requests using deleted tickets
        if ticket_id:
            try:
                tickets.remove(ticket_id)
            except KeyError:
                log.debug("Ticket %s does not exists", ticket_id)
        else:
            tickets.clear()
        return web.response(status=204)
