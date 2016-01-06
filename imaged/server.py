# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from contextlib import contextmanager
from wsgiref import simple_server
import SocketServer
import json
import os
import re
import signal
import ssl
import time

import webob

from webob.exc import (
    HTTPException,
    HTTPBadRequest,
    HTTPMethodNotAllowed,
    HTTPNotFound,
    HTTPForbidden
)

from . import directio
from . import uhttp
from . import util

image_server = None
ticket_server = None
tickets = {}
running = True


def main(args):
    config = Config()
    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)
    start(config)
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
    image_app = Application(config, [(r"/images/(.*)", Images)])
    image_server.set_app(image_app)

    ticket_server = uhttp.UnixWSGIServer(config.socket, UnixWSGIRequestHandler)
    ticket_app = Application(config, [(r"/tickets/(.*)", Tickets)])
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
    socket = "/var/run/vdsm/imaged.sock"

    @property
    def key_file(self):
        return os.path.join(self.pki_dir, "keys", "vdsmkey.pem")

    @property
    def cert_file(self):
        return os.path.join(self.pki_dir, "certs", "vdsmcert.pem")


def error_response(e):
    """
    Return WSGI application for sending error response using JSON format.
    """
    payload = {
        "code": e.code,
        "title": e.title,
        "explanation": e.explanation
    }
    if e.detail:
        payload["detail"] = e.detail
    return response(status=e.code, payload=payload)


def response(status=200, payload=None):
    """
    Return WSGI application for sending response in JSON format.
    """
    body = json.dumps(payload) if payload else ""
    return webob.Response(status=status, body=body,
                          content_type="application/json")


class Application(object):
    """
    WSGI application dispatching requests based on path and method to request
    handlers.
    """

    def __init__(self, config, routes):
        self.config = config
        self.routes = [(re.compile(pattern), cls) for pattern, cls in routes]

    def __call__(self, env, start_response):
        request = webob.Request(env)
        try:
            resp = self.dispatch(request)
        except HTTPException as e:
            resp = error_response(e)
        return resp(env, start_response)

    def dispatch(self, request):
        method_name = request.method.lower()
        if method_name.startswith("_"):
            raise HTTPMethodNotAllowed("Invalid method %r" %
                                       request.method)
        path = request.path_info
        for route, handler_class in self.routes:
            match = route.match(path)
            if match:
                handler = handler_class(self.config, request)
                try:
                    method = getattr(handler, method_name)
                except AttributeError:
                    raise HTTPMethodNotAllowed(
                        "Method %r not defined for %r" %
                        (request.method, path))
                else:
                    request.path_info_pop()
                    return method(*match.groups())
        raise HTTPNotFound("No handler for %r" % path)


def get_ticket(ticket_id, op, size):
    """
    Return a ticket for the requested operation, authorizing the operation.
    """
    try:
        ticket = tickets[ticket_id]
    except KeyError:
        raise HTTPForbidden("No such ticket %r" % ticket_id)
    if ticket["expires"] <= time.time():
        raise HTTPForbidden("Ticket %r expired" % ticket_id)
    if op not in ticket["ops"]:
        raise HTTPForbidden("Ticket %r forbids %r" % (ticket_id, op))
    if size > ticket["size"]:
        raise HTTPForbidden("Content-Length out of allowed range")
    return ticket


@contextmanager
def register_request(ticket, request_id, request):
    """
    Context manager registring a request with a ticket, so requests can be
    canceled when a ticket is revoked or expired.
    """
    requests = ticket.setdefault("requests", {})
    if request_id in requests:
        raise HTTPForbidden("Request id %r exists" % request_id)
    requests[request_id] = request
    try:
        yield
    finally:
        del requests[request_id]


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
        request_id = self.request.params.get("id")
        if not request_id:
            raise HTTPBadRequest("Request id is required")
        size = self.request.content_length
        ticket = get_ticket(ticket_id, "put", size)
        with register_request(ticket, request_id, self):
            # TODO: cancel copy if ticket expired or revoked
            op = directio.Receive(ticket["path"],
                                  self.request.body_file_raw,
                                  size,
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
            new_expires = patch["expires"]
        except KeyError:
            raise HTTPBadRequest("Missing expires key")
        tickets[ticket_id]["expires"] = new_expires
        return response()

    def delete(self, ticket_id):
        # TODO: cancel requests using deleted tickets
        ticket_id = self.request.path_info_peek()
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
