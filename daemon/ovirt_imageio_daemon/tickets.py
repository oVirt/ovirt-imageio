# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import threading
from webob.exc import HTTPForbidden

from ovirt_imageio_daemon import measure
from ovirt_imageio_common import errors
from ovirt_imageio_common import util

from six.moves import urllib_parse

log = logging.getLogger("tickets")
_tickets = {}
supported_schemes = ['file']


class Ticket(object):

    def __init__(self, ticket_dict=None):
        ticket_dict = ticket_dict or {}

        self._uuid = _required(ticket_dict, "uuid")
        self._size = _required(ticket_dict, "size")
        self._ops = _required(ticket_dict, "ops")

        timeout = _required(ticket_dict, "timeout")
        try:
            timeout = int(timeout)
        except ValueError as e:
            raise errors.InvalidTicketParameter("timeout", timeout, e)
        self._expires = int(util.monotonic_time()) + timeout

        url_str = _required(ticket_dict, "url")
        try:
            self._url = urllib_parse.urlparse(url_str)
        except (ValueError, AttributeError, TypeError) as e:
            raise errors.InvalidTicketParameter("url", url_str, e)
        if self._url.scheme not in supported_schemes:
            raise errors.InvalidTicketParameter(
                "url", url_str,
                "Unsupported url scheme: %s" % self._url.scheme)

        self._filename = ticket_dict.get("filename")
        self._operations = []
        self._lock = threading.Lock()

    @property
    def uuid(self):
        return self._uuid

    @property
    def size(self):
        return self._size

    @property
    def url(self):
        return self._url

    @property
    def ops(self):
        return self._ops

    @property
    def expires(self):
        return self._expires

    @property
    def filename(self):
        return self._filename

    def add_operation(self, operation):
        with self._lock:
            self._operations.append(operation)

    def active(self):
        with self._lock:
            return any(op.active for op in self._operations)

    def transferred(self):
        """
        The number of bytes that were transferred so far using this ticket.
        """
        if len(self.ops) > 1:
            # Both read and write, cannot report meaningful value.
            return None

        with self._lock:
            ranges = [measure.Range(op.offset, op.offset + op.done)
                      for op in self._operations]
        merged_ranges = measure.merge_ranges(ranges)
        return sum(len(range) for range in merged_ranges)

    def info(self):
        info = {
            "active": self.active(),
            "expires": self._expires,
            "ops": list(self._ops),
            "size": self._size,
            "timeout": self.expires - int(util.monotonic_time()),
            "url": urllib_parse.urlunparse(self._url),
            "uuid": self._uuid,
        }
        if self.filename:
            info["filename"] = self.filename
        transferred = self.transferred()
        if transferred is not None:
            info["transferred"] = transferred
        return info

    def extend(self, timeout):
        expires = int(util.monotonic_time()) + timeout
        log.info("Extending ticket %s, new expiration in %d",
                 self._uuid, expires)
        self._expires = expires

    def __repr__(self):
        return ("<Ticket "
                "active={active!r} "
                "expires={self.expires!r} "
                "filename={self.filename!r} "
                "ops={self.ops!r} "
                "size={self.size!r} "
                "transferred={transferred!r} "
                "url={url!r} "
                "uuid={self.uuid!r} "
                "at {addr:#x}>"
                ).format(
                    active=self.active(),
                    addr=id(self),
                    self=self,
                    transferred=self.transferred(),
                    url=urllib_parse.urlunparse(self.url)
                )


def _required(d, key):
    if key not in d:
        raise errors.MissingTicketParameter(key)
    return d[key]


def add(ticket_id, ticket):
    """
    Gets a ticket ID and a Ticket object
    and adds it to the tickets' cache.
    """
    log.info("Adding ticket %s", ticket)
    _tickets[ticket_id] = ticket


def remove(ticket_id):
    log.info("Removing ticket %s", ticket_id)
    del _tickets[ticket_id]


def clear():
    log.info("Clearing all tickets")
    _tickets.clear()


def get(ticket_id):
    """
    Gets a ticket ID and returns the proper
    Ticket object from the tickets' cache.
    """
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
    if ticket.expires <= util.monotonic_time():
        raise HTTPForbidden("Ticket %r expired" % ticket_id)
    if op not in ticket.ops:
        raise HTTPForbidden("Ticket %r forbids %r" % (ticket_id, op))
    if size > ticket.size:
        raise HTTPForbidden("Content-Length out of allowed range")
    return ticket
