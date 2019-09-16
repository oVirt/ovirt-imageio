"""
Test the proxy /tickets/ resource.
"""

import base64
import pytest

from six.moves import http_client
from ovirt_imageio_proxy import auth

from . import http


def test_put(proxy_server, signed_ticket):
    resp = http.request(
        proxy_server, "PUT", "/tickets/", body=signed_ticket.data)
    assert resp.status == 200
    ticket = auth.get_ticket(signed_ticket.id)
    assert ticket.url == signed_ticket.url
    assert ticket.id == signed_ticket.id


@pytest.mark.xfail(reason="Fails when reading the response, need to check why")
def test_put_no_body(proxy_server):
    resp = http.request(proxy_server, "PUT", "/tickets/")
    assert resp.status == 403


def test_put_empty(proxy_server):
    resp = http.request(proxy_server, "PUT", "/tickets/", body=b'')
    assert resp.status == 403


def test_put_invalid_base64(proxy_server):
    body = b"this is not a base64 encoded data"
    resp = http.request(proxy_server, "PUT", "/tickets/", body=body)
    assert resp.status == 403


def test_put_invalid_json(proxy_server):
    body = base64.b64encode(b"this is not a json object")
    resp = http.request(proxy_server, "PUT", "/tickets/", body=body)
    assert resp.status == 403


# The following tests require generating a new signed ticket using tiket.TicketEncoder:
# TODO: singed ticket with bad signature
# TODO: singed ticket with wrong signer
# TODO: expired signed ticket
# TODO: future signed ticket
# TODO: invalid content payload


def test_delete(proxy_server, signed_ticket):
    auth.add_signed_ticket(signed_ticket.data)
    resp = http.request(
        proxy_server, "DELETE", "/tickets/%s" % signed_ticket.id)
    assert resp.status == http_client.NO_CONTENT
    with pytest.raises(auth.NoSuchTicket):
        auth.get_ticket(signed_ticket.id)


def test_delete_unknown_ticket(proxy_server):
    resp = http.request(proxy_server, "DELETE", "/tickets/no-such-ticket")
    assert resp.status == http_client.NO_CONTENT


def test_delete_no_ticket_id(proxy_server):
    resp = http.request(proxy_server, "DELETE", "/tickets/")
    assert resp.status == http_client.BAD_REQUEST
