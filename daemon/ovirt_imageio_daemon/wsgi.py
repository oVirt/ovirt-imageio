# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
from wsgiref import simple_server
from six.moves import socketserver

log = logging.getLogger("wsgi")


class WSGIServer(socketserver.ThreadingMixIn,
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

    # Avoids possible delays when sending very small response.
    disable_nagle_algorithm = True

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

    # wsgiref handers ignores the http request handler's protocol_version, and
    # uses its own version. This results in requests returning HTTP/1.0 instead
    # of HTTP/1.1 - see https://bugzilla.redhat.com/1512317
    #
    # Looking at python source we need to define here:
    #
    #   http_version = "1.1"
    #
    # Bug adding this break some tests.
    # TODO: investigate this.

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

    def close(self):
        """
        Extend to close the connection after failures.

        If the request failed but it has a content-length header, there
        is a chance that some of the body was not read yet. Since we
        cannot recover from this, the only thing we can do is closing
        the connection.
        """
        if self.status:
            status = int(self.status[:3])
            if status >= 400 and self.environ["CONTENT_LENGTH"]:
                log.debug("Closing the connection")
                self.request_handler.close_connection = 1

        simple_server.ServerHandler.close(self)
