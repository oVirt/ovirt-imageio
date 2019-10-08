"""
Handling the proxy /tickets/ resource.
"""

import logging

from six.moves import http_client
from webob import exc

from . import auth
from . import web

log = logging.getLogger("tickets")


class RequestHandler(object):
    """
    Request handler for the /tickets/ resource.
    """

    def __init__(self, config, request, clock=None):
        """
        Arguments:
            config (config object): proxy configuration
            request (webob.Request): underlying http request
        """
        self.config = config
        self.request = request
        self.clock = clock

    def put(self, ticket_id=None):
        """
        Verify and add a signed_ticket, allowing transfer to /images/ticket_id.
        """
        try:
            auth.add_signed_ticket(self.request.body)
        except auth.Error as e:
            raise exc.HTTPForbidden("Error verifying signed ticket: %s" % e)
        return web.response()

    def delete(self, ticket_id=None):
        """
        Delete ticket by a specified ticket_id
        """
        if not ticket_id:
            raise exc.HTTPBadRequest("Missing ticket ID")
        try:
            auth.delete_ticket(ticket_id)
        except auth.NoSuchTicket as e:
            log.debug("Ticket %s does not exists", ticket_id)
        return web.response(http_client.NO_CONTENT)
