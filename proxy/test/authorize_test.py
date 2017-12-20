import json
import pytest
import time

from webob import exc

from ovirt_imageio_proxy import auth
from ovirt_imageio_proxy import http_helper

class Request:
    def __init__(self, headers=None):
        self.headers = headers or {}


class RequestHandler(object):
    def __init__(self, request):
        self.request = request
        self.ticket = None
        self.calls = []

    @http_helper.authorize
    def decorated_method(self, ticket_id):
        print "called with:", ticket_id
        self.calls.append(ticket_id)


def teardown_function(f):
    auth._store.clear()


def test_authorize_ticket_not_installed(proxy_server, signed_ticket):
    handler = RequestHandler(Request())
    with pytest.raises(exc.HTTPUnauthorized):
        handler.decorated_method(signed_ticket.id)
    assert handler.ticket is None
    assert handler.calls == []


def test_authorize_invalid_authorization_header(proxy_server, signed_ticket):
    request = Request({"Authorization": "invalid"})
    handler = RequestHandler(request)
    with pytest.raises(exc.HTTPUnauthorized):
        handler.decorated_method(signed_ticket.id)
    assert handler.ticket is None
    assert handler.calls == []


def test_authorize_ticket_installed(proxy_server, signed_ticket):
    auth.add_signed_ticket(signed_ticket.data)
    handler = RequestHandler(Request())
    handler.decorated_method(signed_ticket.id)
    assert handler.ticket == auth.get_ticket(signed_ticket.id)
    assert handler.calls == [signed_ticket.id]


def test_authorize_ticket_installed_invalid_authorization_header(proxy_server,
                                                                 signed_ticket):
    auth.add_signed_ticket(signed_ticket.data)
    request = Request({"Authorization": "invalid"})
    handler = RequestHandler(request)
    handler.decorated_method(signed_ticket.id)
    assert handler.ticket == auth.get_ticket(signed_ticket.id)
    assert handler.calls == [signed_ticket.id]


def test_authorize_authorization_header(proxy_server, signed_ticket):
    request = Request({"Authorization": signed_ticket.data})
    handler = RequestHandler(request)
    handler.decorated_method(signed_ticket.id)
    assert handler.ticket == auth.get_ticket(signed_ticket.id)
    assert handler.calls == [signed_ticket.id]


# Extracted from resources/auth_ticket.out
EXP = 2452291246
NBF = 1452291246


def test_authorize_ticket_expired(proxy_server, signed_ticket, monkeypatch):
    auth.add_signed_ticket(signed_ticket.data)
    handler = RequestHandler(Request())
    monkeypatch.setattr(time, 'time', lambda: EXP+1)
    with pytest.raises(exc.HTTPUnauthorized):
        handler.decorated_method(signed_ticket.id)
    assert handler.calls == []
    assert handler.ticket is None
    # Ticket expired but we don't remove it
    auth.get_ticket(signed_ticket.id)


# We are running internally add signed ticket
@pytest.mark.parametrize("fake_time", [EXP+1, NBF-1])
def test_authorize_ticket_expired__authorization_header(proxy_server,
                                                        signed_ticket,
                                                        monkeypatch,
                                                        fake_time):
    request = Request({"Authorization": signed_ticket.data})
    handler = RequestHandler(request)
    monkeypatch.setattr(time, 'time', lambda: fake_time)
    with pytest.raises(exc.HTTPUnauthorized):
        handler.decorated_method(signed_ticket.id)
    assert handler.calls == []
    assert handler.ticket is None
    with pytest.raises(auth.NoSuchTicket):
        auth.get_ticket(signed_ticket.id)


def test_authorize_ticket_expired_ignore_authorization_header(proxy_server,
                                                              signed_ticket,
                                                              monkeypatch):
    auth.add_signed_ticket(signed_ticket.data)
    handler = RequestHandler(None)
    monkeypatch.setattr(time, 'time', lambda: EXP+1)
    with pytest.raises(exc.HTTPUnauthorized):
        handler.decorated_method(signed_ticket.id)
    assert handler.calls == []
    assert handler.ticket is None