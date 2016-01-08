"""
General session management APIs.  Most session operations should be done in a
context where session_rlock is acquired to prevent race conditions between
threads.
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
HEADER_SESSION_ID = 'X-Session-Id'

# Session-only values
SESSION_ID = 'session-id'
SESSION_PROXY_TICKET = 'proxy-ticket'
SESSION_LAST_ACTIVITY = 'last-activity'

SESSION_EXPIRATION = 'expiration'
SESSION_ISSUED_AT = 'issued-at'
SESSION_IMAGED_HOST_URI = 'imaged-host-uri'
SESSION_RESOURCE_ID = 'resource-id'
SESSION_TRANSFER_TICKET = 'transfer-ticket'


session_rlock = threading.RLock()
_sessions = {}
_tokenmap = {}


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
    token = request.headers.get(HEADER_AUTHORIZATION)
    session_id = request.headers.get(HEADER_SESSION_ID)

    if token:
        with session_rlock:
            existing_token_session_id = _tokenmap.get(token)

        if existing_token_session_id:
            # We've seen this token before and it has an associated session id
            if session_id and existing_token_session_id != session_id:
                raise exc.HTTPBadRequest("Session id must match authorization token")
            session_id = existing_token_session_id
        else:
            # New token; process it and create new or update existing session
            try:
                session_id = _create_update_session(token, session_id)
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
            remove(session_id)
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
    session = {SESSION_ID: sid,
               SESSION_PROXY_TICKET: authorization,
    }
    session.update(ticket_vars)

    logging.info("%s session: %s",
        'Updated' if session_id else 'Established',
        ', '.join("{}: '{}'".format(k, session[k]) for k in sorted(session.keys())))

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

    We could use JSON Web Tokens, but the tools aren't yet widely available.
    Instead, use a JSON-encoded payload inside a Cryptographic Message Syntax
    token (RFC 5652).  This detail is hidden from the caller, who just calls
    this function with the opaque payload and receives a dict of decoded
    values in return.

    :param ticket: payload from request Authorization header
    :return: dict of session values from payload, if token is valid
    :raise ValueError: token is invalid
    """
    if not config.json_proxy_token:
        payload = _decode_ovirt_token(ticket)
    else:
        # For debugging, avoid having to send a signed token
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


def _decode_ovirt_token(payload):
    """
    Decodes and verifies an oVirt token for the caller, returning the
    payload.

    :param payload: token to verify
    :return: verified payload
    :raise ValueError: token is invalid or error verifying token
    """
    # TODO download cert from engine (store url in config)
    signer_cert = config.engine_cert
    if config.verify_certificate:
        ca_cert = config.ca_cert
    else:
        ca_cert = None
        logging.warning("Not verifying certificate!")

    logging.info(signer_cert)
    with open(signer_cert, 'r') as f:
        signer_cert_data = f.read()
    ticketDecoder = ticket.TicketDecoder(ca_cert, None, signer_cert_data)

    try:
        logging.info(payload)
        payload = ticketDecoder.decode(payload)
    except Exception as e:
        logging.error("Failed to verify proxy ticket: %s", str(e))
        raise ValueError("Unable to verify proxy ticket")

    return payload

def _encode_ovirt_token(payload, lifetime_seconds):
    """
    Creates an ovirt token with the given payload.  This will probably
    be used only by standalone tools, not in the main daemon code.

    :param payload: Content to encode
    :return: Signed token
    """
    cert = config.signing_cert
    key = config.signing_key
    ticketEncoder = ticket.TicketEncoder(cert, key, lifetime_seconds)
    try:
        t = ticketEncoder.encode(payload)
    except Exception as e:
        logging.error("Failed to create proxy ticket: %s", str(e))
        raise ValueError("Unable to create proxy ticket")

    return t
