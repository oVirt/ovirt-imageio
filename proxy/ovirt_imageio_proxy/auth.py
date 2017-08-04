"""
General session management APIs.  Most session operations should be done in a
context where session_rlock is acquired to prevent race conditions between
threads.

When a connection to the proxy is established, the client should send an
Authorization header containing the signed ticket provided by the engine which
authorizes image operations for a particular image.  Subsequent requests can
include the session id returned by the proxy after successful authentication.

The session flow is:
- Client connects to the proxy and provides the Authorization header
  containing the signed ticket.
- Proxy calls start_session(), which will decode the ticket and, upon success,
  initiate a new session.  (If The client instead provides a session id, an
  existing session will be validated and used.)
- Proxy stores the session id internally so that it can be used during further
  request processing.
- The proxy performs the requested operation, which may use or update session-
  specific state using get_session(), set_session(), get_session_attribute(),
  or set_session_attribute().
- The proxy updates the session activity timestamp using 
  update_session_activity().
- The proxy returns the session id to the caller if the Authentication header
  was provided via add_response_header().
"""

import json
import logging
import subprocess
import threading
import time
import uuid

from webob import exc

# TODO instance of config, rather than global
import config
import server
import ticket
import util

TOKEN_NOT_BEFORE = 'nbf'
TOKEN_EXPIRATION = 'exp'
TOKEN_ISSUED_AT = 'iat'
TOKEN_IMAGED_HOST_URI = 'imaged-uri'
TOKEN_TRANSFER_TICKET = 'transfer-ticket'

# Header names
HEADER_AUTHORIZATION = 'Authorization'
HEADER_SESSION_ID = 'Session-Id'

# Session-only values
SESSION_ID = 'session-id'
SESSION_PROXY_TICKET = 'proxy-ticket'
SESSION_LAST_ACTIVITY = 'last-activity'

SESSION_EXPIRATION = 'expiration'
SESSION_ISSUED_AT = 'issued-at'
SESSION_IMAGED_HOST_URI = 'imaged-host-uri'
SESSION_RESOURCE_ID = 'resource-id'
SESSION_TRANSFER_TICKET = 'transfer-ticket'

PARAM_SESSION_ID = 'session_id'


session_rlock = threading.RLock()
_sessions = {}


def get_session(session_id):
    """ Return a reference to a Session object """
    return _sessions.get(session_id)


def set_session(session_id, content):
    _sessions[session_id] = content


def remove(session_id):
    del _sessions[session_id]


def get_session_attribute(request, name):
    with session_rlock:
        return get_session(request.headers[HEADER_SESSION_ID]).get(name)


def set_session_attribute(request, name, value):
    with session_rlock:
        get_session(request.headers[HEADER_SESSION_ID])[name] = value


# TODO unused
class Session(object):
    """ Object to hold attributes for a session, with getter/setter methods. """
    def __init__(self):
        self.attributes = {}

    def get_attribute(self, name):
        """ Return the named session attribute. """
        return self.attributes.get(name)

    def set_attribute(self, name, value):
        """ Set the named session attribute. """
        self.attributes[name] = value


def start_session(request):
    """
    Verify that a valid session exists for this request, either by checking
    validity of a session indicated by a session id in this request, or by
    creating a session from data in the request headers.

    :param request: webob request
    :return: session id
    :raise ValueError: Invalid session or invalid parameters to create the session
        (cause will be in e.message)
    """
    # Note that webob.headers is case-insensitive
    ticket = request.headers.get(HEADER_AUTHORIZATION)
    session_id = request.headers.get(HEADER_SESSION_ID)
    if session_id is None:
        session_id = request.params.get(PARAM_SESSION_ID)

    if ticket:
        # New ticket; process it and create new or update existing session
        try:
            session_id = _create_update_session(ticket, session_id)
        except ValueError as e:
            logging.error("Error starting session: " + e.message, exc_info=True)
            raise exc.HTTPUnauthorized("Could not initialize session: " + e.message)

    if not session_id:
        raise exc.HTTPUnauthorized("Not authorized")

    if session_is_valid(session_id):
        if HEADER_SESSION_ID in request.headers:
            logging.info("Resuming session %s", session_id)
    else:
        # TODO a separate thread should periodically sweep for expired sessions
        with session_rlock:
            try:
                remove(session_id)
            except KeyError as e:
                logging.debug("No such session %r", session_id)
        raise exc.HTTPUnauthorized("Invalid session id or session expired")

    request.headers[HEADER_SESSION_ID] = session_id
    return session_id


def update_session_activity(request):
    """
    Update session attributes at the end of a request.  Currently only updates
    the session activity time.

    :param request: webob.Request
    :return: nothing
    """
    if get_session(request.headers[HEADER_SESSION_ID]):
        set_session_attribute(request, SESSION_LAST_ACTIVITY, time.time())


def session_is_valid(session_id):
    """
    Verify validity of a session.

    :param session_id: session UUID
    :return: true if valid, false otherwise
    """
    with session_rlock:
        session = get_session(session_id)
        if session and (time.time() - config.allowed_skew_seconds
                        < session[SESSION_EXPIRATION]):
            return True
        else:
            return False


def _create_update_session(authorization, session_id=None):
    """
    Initialize a session based on the provided authorization header. If
    a session id is provided, the session will be updated, otherwise a
    new session is created

    :param authorization: contents of request authorization header
    :return: session_id
    :raise ValueError: session could not be initialized
                       from the request, see e.message
    """
    ticket_vars = _decode_proxy_ticket(authorization)

    sid = session_id if session_id else str(uuid.uuid4())
    session = {
        SESSION_ID: sid,
        SESSION_PROXY_TICKET: authorization,
    }
    session.update(ticket_vars)

    def trim(s, max):
        if type(s) == str and len(s) > max:
            return s[:(max/2)] + '...' + s[len(s)-(max/2):]
        else:
            return s

    logging.info("%s session: %s",
                 'Updated' if session_id else 'Established',
                 ', '.join("{}: '{}'".format(k, trim(session[k], 120))
                           for k in sorted(session.keys())))

    with session_rlock:
        if session_id:
            get_session(sid).update(session)
        else:
            set_session(sid, session)
    return sid


def _decode_proxy_ticket(ticket):
    """
    Decodes and verifies signature of proxy ticket.  If valid, returns a dict
    of session variables retrieved from the ticket.

    The ticket is a JSON payload inside an oVirt ticket created by the
    org.ovirt.engine.core.uutils.crypto.ticket.TicketEncoder class; however,
    this is opaque to the caller, who receives such a ticket from the engine
    upon starting the image transfer operation and passes it to the proxy.

    :param ticket: payload from request Authorization header
    :return: dict of session values from payload, if ticket is valid
    :raise ValueError: ticket is invalid
    """
    if config.signed_proxy_ticket:
        payload = _decode_ovirt_ticket(ticket)
    else:
        # For debugging, avoid having to send a signed ticket
        payload = ticket
    logging.debug("Decoded ticket: %r", payload)

    try:
        decoded = json.loads(payload)
    except Exception as e:
        raise ValueError("Invalid ticket: {}".format(str(e)))

    # TODO ovirt tickets appear to check some of this for us
    required = {
        TOKEN_EXPIRATION,
        TOKEN_NOT_BEFORE,
        TOKEN_ISSUED_AT,
        TOKEN_IMAGED_HOST_URI,
        TOKEN_TRANSFER_TICKET,
        }
    missing = required - decoded.viewkeys()
    if missing:
        logging.error("Invalid ticket: %s", decoded)
        raise ValueError("Required values missing from proxy ticket: " +
                         ', '.join(missing))

    now = time.time()

    if decoded[TOKEN_NOT_BEFORE] > now - config.allowed_skew_seconds:
        logging.error(
            "Proxy ticket not yet valid: %d > %d - %d",
            decoded[TOKEN_NOT_BEFORE], now, config.allowed_skew_seconds
        )
        logging.error("Invalid ticket: %s", decoded)
        raise ValueError("Proxy ticket not yet valid")

    if decoded[TOKEN_EXPIRATION] <= now + config.allowed_skew_seconds:
        logging.error(
            "Proxy ticket expired: %d < %d + %d",
            decoded[TOKEN_EXPIRATION], now, config.allowed_skew_seconds
        )
        logging.error("Invalid ticket: %s", decoded)
        raise ValueError("Proxy ticket has expired")

    logging.info("Proxy ticket valid: %s", decoded)

    ret = {
        SESSION_EXPIRATION:      decoded[TOKEN_EXPIRATION],
        SESSION_IMAGED_HOST_URI: decoded[TOKEN_IMAGED_HOST_URI],
        SESSION_TRANSFER_TICKET: decoded[TOKEN_TRANSFER_TICKET],
    }
    return ret


def _decode_ovirt_ticket(payload):
    """
    Decodes and verifies an oVirt ticket for the caller, returning the
    payload.

    :param payload: ticket to verify
    :return: verified payload
    :raise ValueError: ticket is invalid or error verifying ticket
    """
    # TODO download cert from engine (store url in config)
    signer_cert = config.engine_cert_file
    if config.verify_certificate:
        ca_cert = config.engine_ca_cert_file
    else:
        ca_cert = None
        logging.warning("Not verifying certificate!")

    with open(signer_cert, 'r') as f:
        signer_cert_data = f.read()
    ticketDecoder = ticket.TicketDecoder(ca_cert, None, signer_cert_data)

    try:
        payload = ticketDecoder.decode(payload)
    except Exception as e:
        logging.error("Failed to verify proxy ticket: %s", str(e))
        raise ValueError("Unable to verify proxy ticket")

    return payload
