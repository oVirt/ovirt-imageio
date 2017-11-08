"""
Proxy authorization.
"""

import json
import logging
import time

from . import config
from . import ticket

log = logging.getLogger("auth2")


_store = {}


class Error(Exception):
    """
    Base class for authorication exceptions.
    """
    msg = "Base class for auth2 errors"

    def __str__(self):
        return self.msg.format(self=self)


class DecodeError(Error):
    msg = "Error decoding ticket: {self.reason}"

    def __init__(self, reason):
        self.reason = reason


class InvalidOvirtTicket(Error):
    msg = "Invalid ovirt ticket (data={self.data!r}, reason={self.reason})"

    def __init__(self, data, reason):
        self.data = data
        self.reason = reason


class InvalidProxyTicket(Error):
    msg = "Invalid proxy ticket (ticket={self.ticket!r}, reason={self.reason})"

    def __init__(self, ticket, reason):
        self.ticket = ticket
        self.reason = reason


class RequiredKeyMissing(Error):
    msg = ("Required proxy ticket key missing (key={self.key}, "
           "ticket={self.ticket})")

    def __init__(self, key, ticket):
        self.key = key
        self.ticket = ticket


class ExpiredProxyTicket(Error):
    msg = ("Proxy ticket has expired (expires={self.expires}, "
           "now={self.now}, skew={self.skew})")

    def __init__(self, expires, now, skew):
        self.expires = expires
        self.now = now
        self.skew = skew


class FutureProxyTicket(Error):
    msg = ("Proxy ticket not valid yet (nbf={self.nbf}, "
           "now={self.now}, skew={self.skew})")

    def __init__(self, nbf, now, skew):
        self.nbf = nbf
        self.now = now
        self.skew = skew


class NoSuchTicket(Error):
    msg = "No such ticket (ticket_id={self.ticket_id})"

    def __init__(self, ticket_id):
        self.ticket_id = ticket_id


class Ticket(object):
    """
    A valid proxy ticket.
    """

    def __init__(self, text):
        try:
            d = json.loads(text)
        except ValueError as e:
            raise InvalidProxyTicket(text, e)
        try:
            # Validate ticket expires and nbf times. This requires clock some
            # syncronization on the engine and proxy hosts. Differences in
            # clocks are mitigated by config.allow_skew_seoconds.

            now = int(time.time())
            not_before = d["nbf"]
            expires = d["exp"]

            if not_before > now - config.allowed_skew_seconds:
                raise FutureProxyTicket(
                    not_before, now, config.allowed_skew_seconds)

            if expires <= now + config.allowed_skew_seconds:
                raise ExpiredProxyTicket(
                    expires, now, config.allowed_skew_seconds)

            # Extract the ticket data.

            self._id = d["transfer-ticket"]
            self._url = d["imaged-uri"]
        except KeyError as e:
            raise RequiredKeyMissing(e, d)
        except TypeError as e:
            raise InvalidProxyTicket(d, e)

    @property
    def id(self):
        return self._id

    @property
    def url(self):
        return self._url

    def __repr__(self):
        return ("<Ticket "
                "id={self.id!r}, "
                "url={self.url!r} "
                "at {addr:#x}>").format(self=self, addr=id(self))


def get_ticket(ticket_id):
    """
    Lookup a ticket by ticket id.

    Arguments:
        ticket_id (str): ticket id

    Returns:
        Authentication ticket (auth2.Ticket)

    Raises:
        auth2.NoSuchTicket if the ticket_id is not in the store
    """
    try:
        return _store[ticket_id]
    except KeyError:
        raise NoSuchTicket(ticket_id)


def add_signed_ticket(data):
    """
    Add a signed ticket to the ticket store.

    Arguments:
        data (bytes): signed ticket data

    Raises:
        auth2.Error if the ticket is not valid
    """
    payload = _decode_ovirt_ticket(data)
    log.debug("Received payload: %r", payload)
    proxy_ticket = Ticket(payload)
    log.info("Adding new ticket: %s", proxy_ticket)
    _store[proxy_ticket.id] = proxy_ticket


def delete_ticket(ticket_id):
    """
    Delete a ticket from the ticket store.

    Arguments:
        ticket_id (str): the ID of the ticket

    Raises:
        auth2.NoSuchTicket if the ticket_id is not in the store
    """
    log.info("Deleting ticket %r", ticket_id)
    try:
        del _store[ticket_id]
    except KeyError:
        raise NoSuchTicket(ticket_id)


def _decode_ovirt_ticket(data):
    """
    Decode and verify ovirt signed ticket, and return the underlying ticket
    payload.

    Arguments:
        data (bytes): signed ticket data

    Returns:
        The underlying ticket data (str)

    Raises:
        auth2.Error if the signed ticket is not valid or could not be decoded.
    """
    try:
        with open(config.engine_cert_file, 'rb') as f:
            signer_cert_data = f.read()
    except EnvironmentError as e:
        raise DecodeError("Cannot read engine certificate: %s" % e)

    try:
        decoder = ticket.TicketDecoder(
            config.engine_ca_cert_file, None, signer_cert_data)
        return decoder.decode(data)
    except (ValueError, TypeError) as e:
        raise InvalidOvirtTicket(data, e)
