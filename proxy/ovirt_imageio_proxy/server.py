#
# ovirt-imageio-proxy - oVirt image upload proxy
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import httplib
import SocketServer
import threading
from wsgiref import simple_server

import webob

from ovirt_imageio_common import ssl
from ovirt_imageio_common import web

from . import images
from . import info
from . import tickets


class Server:
    _image_server = None

    def __init__(self):
        pass

    def start(self, config):
        server = ThreadedWSGIServer((config.host, config.port),
                                    WSGIRequestHandler)
        if config.use_ssl:
            self._secure_server(config, server)
        app = web.Application(config, [
            (r"/images/(.*)", images.RequestHandler),
            (r"/tickets/(.*)", tickets.RequestHandler),
            (r"/info/", info.RequestHandler),
        ])
        server.set_app(app)
        self._start_server(config, server, "image.server")
        self._image_server = server

    def stop(self):
        self._image_server.shutdown()
        self._image_server = None

    def _secure_server(self, config, server):
        # TODO consider cert_reqs
        context = ssl.server_context(config.ssl_cert_file,
                                     config.ssl_cert_file,
                                     config.ssl_key_file)
        server.socket = context.wrap_socket(server.socket, server_side=True)

    def _start_server(self, config, server, name):
        def run():
            server.serve_forever(poll_interval=config.poll_interval)

        t = threading.Thread(target=run, name=name)
        t.daemon = True
        t.start()


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
