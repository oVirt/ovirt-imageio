"""
Demo WSGI based server.

To run the server:

    export PYTHONPATH=.:../daemon

    python test/wsgidemo.py

TODO: remove when we remove the wsgi module.
"""

import logging
import webob
from ovirt_imageio_common import web
from ovirt_imageio_daemon import wsgi


class Bench(object):

    def __init__(self, config, request, clock=None):
        self.request = request

    def get(self, name):
        body = b"%s\n" % name.encode("utf-8")
        return webob.Response(body=body)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s (%(threadName)s) %(message)s")

logging.info("Starting server on port %s", 8000)

server = wsgi.WSGIServer(("", 8000), wsgi.WSGIRequestHandler)
app = web.Application(None, [(r"/bench/(.*)", Bench)])
server.set_app(app)
server.serve_forever()
