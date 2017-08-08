import httplib
import server

from webob.exc import HTTPNotFound

from ovirt_imageio_common import web

from . import auth

from . http_helper import (
    addcors,
    requiresession,
)


class RequestHandler(object):
    """
    Request handler for the /sessions/ resource.
    """

    def __init__(self, config, request):
        """
        :param config: config.py
        :param request: http request
        """
        self.config = config
        self.request = request

    @requiresession
    @addcors
    def post(self, res_id):
        """ Creates a new session and returns its ID
        # POST http://<proxy>/sessions/
        # Request Headers: { Authorization: <signed_ticket> }
        # Response Headers: { 'session_id': session_id }
        """
        session_id = auth.get_session_attribute(self.request, auth.SESSION_ID)
        response = server.response(200)
        response.headers[auth.SESSION_ID] = session_id
        return response

    @addcors
    def options(self, res_id):
        return web.response(httplib.NO_CONTENT)

    @addcors
    def delete(self, res_id):
        try:
            auth.remove(res_id)
        except KeyError as e:
            raise HTTPNotFound("No such session %r" % res_id)
        response = server.response(204)
        return response