# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
from webob.exc import HTTPForbidden
from ovirt_imageio_common import util

log = logging.getLogger("tickets")
_tickets = {}


def add(ticket_id, ticket):
    log.info("Adding ticket %s", ticket)
    _tickets[ticket_id] = ticket


def remove(ticket_id):
    log.info("Removing ticket %s", ticket_id)
    del _tickets[ticket_id]


def clear():
    log.info("Clearing all tickets")
    _tickets.clear()


def get(ticket_id):
    return _tickets[ticket_id]


def authorize(ticket_id, op, size):
    """
    Authorizing a ticket operation
    """
    log.debug("Authorizing %r to offset %d for ticket %s",
              op, size, ticket_id)
    try:
        ticket = _tickets[ticket_id]
    except KeyError:
        raise HTTPForbidden("No such ticket %r" % ticket_id)
    if ticket["expires"] <= util.monotonic_time():
        raise HTTPForbidden("Ticket %r expired" % ticket_id)
    if op not in ticket["ops"]:
        raise HTTPForbidden("Ticket %r forbids %r" % (ticket_id, op))
    if size > ticket["size"]:
        raise HTTPForbidden("Content-Length out of allowed range")
    return ticket
