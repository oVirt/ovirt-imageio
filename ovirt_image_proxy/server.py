#
# ovirt-image-proxy - oVirt image upload proxy
# Copyright (C) 2015 Red Hat, Inc.
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
import logging
import re
import SocketServer
import ssl
import threading
from wsgiref import simple_server

import webob
from webob import exc

import download_handler
from http_helper import (
    httplog,
    addcors,
)
import image_handler


class Server:
    _image_server = None

    def __init__(self):
        pass

    def start(self, config):
        images = image_handler.ImageHandler
        # TODO create downloadhandler to broker requests to downloader thread(s)
        downloads = download_handler.DownloadHandler

        server = ThreadedWSGIServer((config.host, config.port), WSGIRequestHandler)
        if config.use_ssl:
            self._secure_server(config, server)
        server.set_app(Application(config,
                                   [(r"/images/(.*)", images),
                                   (r"/downloads/(.*)", downloads)]))
        self._start_server(config, server, "image.server")
        self._image_server = server

    def stop(self):
        self._image_server.shutdown()
        self._image_server = None

    def _secure_server(self, config, server):
        # TODO consider cert_reqs
        server.socket = ssl.wrap_socket(server.socket, certfile=config.cert_file,
                                        keyfile=config.key_file, server_side=True)

    def _start_server(self, config, server, name):
        def run():
            server.serve_forever(poll_interval=config.poll_interval)

        t = threading.Thread(target=run, name=name)
        t.daemon = True
        t.start()


def _error_response(status=httplib.INTERNAL_SERVER_ERROR, message=None):
    return response(status, message)


def response(status=httplib.OK, message=None):
    body = message if message else ''
    if body and not body.endswith('\n'):
        body += '\n'
    return webob.Response(status=status, body=body, content_type='text/plain')


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
        resp = self.handle_request(request)
        return resp(env, start_response)

    @httplog
    @addcors
    def handle_request(self, request):
        try:
            resp = self.dispatch(request)
        except exc.HTTPException as e:
            resp = _error_response(e.code, e.explanation)
        return resp

    def dispatch(self, request):
        if request.method not in ('GET', 'PUT', 'PATCH', 'POST', 'DELETE', 'OPTIONS', 'HEAD'):
            raise exc.HTTPMethodNotAllowed("Invalid method %r" %
                                       request.method)
        path = request.path_info
        for route, handler_class in self.routes:
            match = route.match(path)
            if match:
                # TODO not sure about passing config here vs module-level config
                handler = handler_class(self.config)
                try:
                    method = getattr(handler, request.method.lower())
                except AttributeError:
                    raise exc.HTTPMethodNotAllowed(
                        "Method %r not defined for %r" %
                        (request.method, path))
                else:
                    request.path_info_pop()
                    try:
                        resp = method(request)
                    except exc.HTTPException as e:
                        resp = _error_response(e.code, e.explanation)
                    except Exception as e:
                        # Catch anything that might have slipped through
                        s = "Internal error: " + e.message
                        logging.error(s, exc_info=True)
                        resp = _error_response(httplib.INTERNAL_SERVER_ERROR, s)
                    return resp
        raise exc.HTTPNotFound("No handler for %r" % path)


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
