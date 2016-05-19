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

from . import uhttp


image_server = None
ticket_server = None
tickets = {}
running = True
supported_schemes = ['file']


def main(args):
    config = Config()
    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)
    start(config)
    systemd.daemon.notify("READY=1")
    try:
        while running:
            time.sleep(30)
    finally:
        stop()


def terminate(signo, frame):
    global running
    running = False


def start(config):
    global image_server, ticket_server
    assert not (image_server or ticket_server)

    image_server = ThreadedWSGIServer((config.host, config.port),
                                      WSGIRequestHandler)
    secure_server(config, image_server)
    image_app = web.Application(config, [(r"/images/(.*)", Images)])
    image_server.set_app(image_app)

    ticket_server = uhttp.UnixWSGIServer(config.socket, UnixWSGIRequestHandler)
    ticket_app = web.Application(config, [(r"/tickets/(.*)", Tickets)])
    ticket_server.set_app(ticket_app)

    start_server(config, image_server, "image.server")
    start_server(config, ticket_server, "ticket.server")


def stop():
    global image_server, ticket_server
    image_server.shutdown()
    ticket_server.shutdown()
    image_server = None
    ticket_server = None


def secure_server(config, server):
    server.socket = ssl.wrap_socket(server.socket, certfile=config.cert_file,
                                    keyfile=config.key_file, server_side=True)


def start_server(config, server, name):
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

    def __init__(self, config, request):
        self.config = config
        self.request = request

    def put(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")
        content_range = web.content_range(self.request)
        offset = content_range.start or 0
        size = self.request.content_length
        ticket = get_ticket(ticket_id, "write", offset + size)
        # TODO: cancel copy if ticket expired or revoked
        op = directio.Receive(ticket["url"].path,
                              self.request.body_file_raw,
                              size,
                              offset=offset,
                              buffersize=self.config.buffer_size)
        op.run()
        return response()


class Tickets(object):
    """
    Request handler for the /tickets/ resource.
    """

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


class UnixWSGIRequestHandler(uhttp.UnixWSGIRequestHandler):
    """
    WSGI over unix domain socket request handler using HTTP/1.1.
    """
    protocol_version = "HTTP/1.1"
