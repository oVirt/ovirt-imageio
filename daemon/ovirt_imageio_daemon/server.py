# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from ovirt_imageio_common import web

from wsgiref import simple_server
import SocketServer
import json
import os
import signal
import ssl
import urlparse
import time
import logging
import logging.config

import systemd.daemon
import webob

from webob.exc import (
    HTTPException,
    HTTPBadRequest,
    HTTPMethodNotAllowed,
    HTTPNotFound,
    HTTPForbidden
)

from ovirt_imageio_common import directio
from ovirt_imageio_common import util
from ovirt_imageio_common import version

from . import uhttp

CONF_DIR = "/etc/ovirt-imageio-daemon"

log = logging.getLogger("server")
image_server = None
ticket_server = None
tickets = {}
running = True
supported_schemes = ['file']


def main(args):
    configure_logger()
    log.info("Starting (version %s)", version.string)
    config = Config()
    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)
    start(config)
    systemd.daemon.notify("READY=1")
    log.info("Ready for requests")
    try:
        while running:
            time.sleep(30)
    finally:
        stop()
    log.info("Stopped")


def configure_logger():
    conf = os.path.join(CONF_DIR, "logger.conf")
    logging.config.fileConfig(conf, disable_existing_loggers=False)


def terminate(signo, frame):
    global running
    log.info("Received signal %d, shutting down", signo)
    running = False


def start(config):
    global image_server, ticket_server
    assert not (image_server or ticket_server)

    log.debug("Starting images service on port %d", config.port)
    image_server = ThreadedWSGIServer((config.host, config.port),
                                      WSGIRequestHandler)
    secure_server(config, image_server)
    image_app = web.Application(config, [(r"/images/(.*)", Images)])
    image_server.set_app(image_app)
    start_server(config, image_server, "image.server")

    log.debug("Starting tickets service on socket %s", config.socket)
    ticket_server = uhttp.UnixWSGIServer(config.socket, UnixWSGIRequestHandler)
    ticket_app = web.Application(config, [(r"/tickets/(.*)", Tickets)])
    ticket_server.set_app(ticket_app)
    start_server(config, ticket_server, "ticket.server")


def stop():
    global image_server, ticket_server
    log.debug("Stopping services")
    image_server.shutdown()
    ticket_server.shutdown()
    image_server = None
    ticket_server = None


def secure_server(config, server):
    log.debug("Securing server (certfile=%s, keyfile=%s)",
              config.cert_file, config.key_file)
    server.socket = ssl.wrap_socket(server.socket, certfile=config.cert_file,
                                    keyfile=config.key_file, server_side=True)


def start_server(config, server, name):
    log.debug("Starting thread %s", name)
    util.start_thread(server.serve_forever,
                      kwargs={"poll_interval": config.poll_interval},
                      name=name)


class Config(object):

    pki_dir = "/etc/pki/vdsm"
    host = ""
    port = 54322
    poll_interval = 1.0
    buffer_size = 1024 * 1024
    socket = "/var/run/vdsm/ovirt-imageio-daemon.sock"

    @property
    def key_file(self):
        return os.path.join(self.pki_dir, "keys", "vdsmkey.pem")

    @property
    def cert_file(self):
        return os.path.join(self.pki_dir, "certs", "vdsmcert.pem")


def response(status=200, payload=None):
    """
    Return WSGI application for sending response in JSON format.
    """
    body = json.dumps(payload) if payload else ""
    return webob.Response(status=status, body=body,
                          content_type="application/json")


def get_ticket(ticket_id, op, size):
    """
    Return a ticket for the requested operation, authorizing the operation.
    """
    try:
        ticket = tickets[ticket_id]
    except KeyError:
        raise HTTPForbidden("No such ticket %r" % ticket_id)
    if ticket["expires"] <= util.monotonic_time():
        raise HTTPForbidden("Ticket %r expired" % ticket_id)
    if op not in ticket["ops"]:
        raise HTTPForbidden("Ticket %r forbids %r" % (ticket_id, op))
    if size > ticket["size"]:
        raise HTTPForbidden("Content-Length out of allowed range")
    return ticket


class Images(object):
    """
    Request handler for the /images/ resource.
    """
    log = logging.getLogger("images")

    def __init__(self, config, request):
        self.config = config
        self.request = request

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
        ticket = get_ticket(ticket_id, "write", offset + size)
        # TODO: cancel copy if ticket expired or revoked
        self.log.info("Writing %d bytes at offset %d to %s for ticket %s",
                      size, offset, ticket["url"].path, ticket_id)
        op = directio.Receive(ticket["url"].path,
                              self.request.body_file_raw,
                              size,
                              offset=offset,
                              buffersize=self.config.buffer_size)
        op.run()
        return response()

    def get(self, ticket_id):
        # TODO: cancel copy if ticket expired or revoked
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        # TODO: send entire content (using ticket size) if range not specified?
        if not self.request.range:
            raise HTTPBadRequest("Range header is required")
        # TODO: support partial range (e.g. bytes=0-*)
        offset = self.request.range.start
        size = self.request.range.end - offset
        ticket = get_ticket(ticket_id, "read", offset + size)
        self.log.info("Reading %d bytes at offset %d from %s for ticket %s",
                      size, offset, ticket["url"].path, ticket_id)
        op = directio.Send(ticket["url"].path,
                           None,
                           size,
                           offset=offset,
                           buffersize=self.config.buffer_size)
        resp = webob.Response(
            status=206,
            app_iter=op,
            content_type="application/octet-stream",
            content_length=str(size),
            content_range=self.request.range.content_range(size),
        )
        return resp


class Tickets(object):
    """
    Request handler for the /tickets/ resource.
    """
    log = logging.getLogger("tickets")

    def __init__(self, config, request):
        self.config = config
        self.request = request

    def get(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            ticket = tickets[ticket_id]
        except KeyError:
            raise HTTPNotFound("No such ticket %r" % ticket_id)
        ticket = ticket.copy()
        ticket["url"] = urlparse.urlunparse(ticket["url"])
        self.log.info("Retrieving ticket %s", ticket_id)
        return response(payload=ticket)

    def put(self, ticket_id):
        # TODO
        # - reject invalid or expired ticket
        # - start expire timer
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        try:
            ticket = self.request.json
        except ValueError as e:
            raise HTTPBadRequest("Invalid ticket: %s" % e)
        try:
            timeout = ticket["timeout"]
        except KeyError:
            raise HTTPBadRequest("Missing timeout key")
        try:
            timeout = int(timeout)
        except ValueError as e:
            raise HTTPBadRequest("Invalid timeout value: %s" % e)
        try:
            url_str = ticket["url"]
        except KeyError:
            raise HTTPBadRequest("Missing url key in ticket")
        try:
            ticket["url"] = urlparse.urlparse(url_str)
        except (ValueError, AttributeError, TypeError):
            raise HTTPBadRequest("Invalid url string %r" % url_str)

        if ticket["url"].scheme not in supported_schemes:
            raise HTTPBadRequest("url scheme is not supported "
                                 "for url: %s" % ticket["url"].scheme)

        ticket["expires"] = int(util.monotonic_time()) + timeout
        tickets[ticket_id] = ticket

        self.log.info("Adding ticket %s, expires in %d, image url: %s",
                      ticket_id, ticket["expires"], ticket["url"])
        return response()

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
            ticket = tickets[ticket_id]
        except KeyError:
            raise HTTPNotFound("No such ticket: %s" % ticket_id)
        ticket["expires"] = int(util.monotonic_time()) + timeout
        self.log.info("Extending ticket %s, new expiration in %d",
                      ticket_id, ticket["expires"])
        return response()

    def delete(self, ticket_id):
        # TODO: cancel requests using deleted tickets
        if ticket_id:
            try:
                del tickets[ticket_id]
            except KeyError:
                raise HTTPNotFound("No such ticket %r" % ticket_id)
        else:
            tickets.clear()
        self.log.info("Deleting ticket %s", ticket_id)
        return response(status=204)


class ThreadedWSGIServer(SocketServer.ThreadingMixIn,
                         simple_server.WSGIServer):
    """
    Threaded WSGI HTTP server.
    """
    daemon_threads = True


class WSGIRequestHandler(simple_server.WSGIRequestHandler):
    """
    WSGI request handler using HTTP/1.1.
    """

    protocol_version = "HTTP/1.1"

    def address_string(self):
        """
        Override to avoid slow and unneeded name lookup.
        """
        return self.client_address[0]

    def handle(self):
        """
        Override to use fixed ServerHandler.

        Copied from wsgiref/simple_server.py, using our ServerHandler.
        """
        self.raw_requestline = self.rfile.readline(65537)
        if len(self.raw_requestline) > 65536:
            self.requestline = ''
            self.request_version = ''
            self.command = ''
            self.send_error(414)
            return

        if not self.parse_request():  # An error code has been sent, just exit
            return

        handler = ServerHandler(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self      # backpointer for logging
        handler.run(self.server.get_app())

    def log_message(self, format, *args):
        """
        Override to avoid unwanted logging to stderr.
        """


class ServerHandler(simple_server.ServerHandler):

    def write(self, data):
        """
        Override to allow writing buffer object.

        Copied from wsgiref/handlers.py, removing the check for StringType.
        """
        if not self.status:
            raise AssertionError("write() before start_response()")

        elif not self.headers_sent:
            # Before the first output, send the stored headers
            self.bytes_sent = len(data)    # make sure we know content-length
            self.send_headers()
        else:
            self.bytes_sent += len(data)

        self._write(data)
        self._flush()


class UnixWSGIRequestHandler(uhttp.UnixWSGIRequestHandler):
    """
    WSGI over unix domain socket request handler using HTTP/1.1.
    """
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        """
        Override to avoid unwanted logging to stderr.
        """
