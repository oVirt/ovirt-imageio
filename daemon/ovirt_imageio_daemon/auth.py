# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import bisect
import logging
import threading

from ovirt_imageio_common import backends
from ovirt_imageio_common import errors
from ovirt_imageio_common import util
from ovirt_imageio_daemon import measure

import six
from six.moves import urllib_parse

log = logging.getLogger("tickets")
_tickets = {}


class Ticket(object):

    def __init__(self, ticket_dict):
        if not isinstance(ticket_dict, dict):
            raise errors.InvalidTicket(
                "Invalid ticket: %r, expecting a dict" % ticket_dict)

        self._uuid = _required(ticket_dict, "uuid", six.string_types)
        self._size = _required(ticket_dict, "size", six.integer_types)
        self._ops = _required(ticket_dict, "ops", list)

        self._timeout = _required(ticket_dict, "timeout", six.integer_types)
        now = int(util.monotonic_time())
        self._expires = now + self._timeout
        self._access_time = now

        url_str = _required(ticket_dict, "url", six.string_types)
        try:
            self._url = urllib_parse.urlparse(url_str)
        except (ValueError, AttributeError, TypeError) as e:
            raise errors.InvalidTicketParameter("url", url_str, e)
        if not backends.supports(self._url.scheme):
            raise errors.InvalidTicketParameter(
                "url", url_str,
                "Unsupported url scheme: %s" % self._url.scheme)

        self._transfer_id = _optional(
            ticket_dict, "transfer_id", six.string_types)
        self._filename = _optional(ticket_dict, "filename", six.string_types)
        self._sparse = _optional(ticket_dict, "sparse", bool, default=False)

        self._operations = []
        self._lock = threading.Lock()

        # Set holding ongoing operations.
        self._ongoing = set()

        # Ranges transferred by completed operations.
        self._completed = []

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
    def transfer_id(self):
        """
        Return the ticket transfer id, available since engine 4.2.7 or None
        if the ticket was generated by older engine.
        """
        return self._transfer_id

    @property
    def filename(self):
        return self._filename

    @property
    def sparse(self):
        return self._sparse

    @property
    def idle_time(self):
        """
        Return the time in which the ticket became inactive.
        """
        if self.active():
            return 0
        return int(util.monotonic_time()) - self._access_time

    def run(self, operation):
        """
        Run an operation, binding it to the ticket.
        """
        self._add_operation(operation)
        try:
            operation.run()
        finally:
            self._remove_operation(operation)

    def touch(self):
        """
        Extend the ticket and update the last access time.

        Must be called when an operation is completed.
        """
        now = int(util.monotonic_time())
        self._expires = now + self._timeout
        self._access_time = now

    def _add_operation(self, op):
        with self._lock:
            self._ongoing.add(op)

    def _remove_operation(self, op):
        with self._lock:
            self._ongoing.remove(op)
            r = measure.Range(op.offset, op.offset + op.done)
            bisect.insort(self._completed, r)
            self._completed = measure.merge_ranges(self._completed)
        self.touch()

    def active(self):
        return bool(self._ongoing)

    def transferred(self):
        """
        The number of bytes that were transferred so far using this ticket.
        """
        if len(self.ops) > 1:
            # Both read and write, cannot report meaningful value.
            return None

        with self._lock:
            # NOTE: this must not modify the ticket state.
            completed = [measure.Range(r.start, r.end)
                         for r in self._completed]
            ongoing = [measure.Range(op.offset, op.offset + op.done)
                       for op in self._ongoing]

        ranges = sorted(completed + ongoing)
        ranges = measure.merge_ranges(ranges)
        return sum(len(r) for r in ranges)

    def may(self, op):
        if op == "read":
            # Having "write" imply also "read".
            return "read" in self.ops or "write" in self.ops
        else:
            return op in self.ops

    def info(self):
        info = {
            "active": self.active(),
            "expires": self._expires,
            "idle_time": self.idle_time,
            "ops": list(self._ops),
            "size": self._size,
            "sparse": self._sparse,
            "timeout": self._timeout,
            "url": urllib_parse.urlunparse(self._url),
            "uuid": self._uuid,
        }
        if self._transfer_id:
            info["transfer_id"] = self._transfer_id
        if self.filename:
            info["filename"] = self.filename
        transferred = self.transferred()
        if transferred is not None:
            info["transferred"] = transferred
        return info

    def extend(self, timeout):
        expires = int(util.monotonic_time()) + timeout
        self._expires = expires

    def __repr__(self):
        return ("<Ticket "
                "active={active!r} "
                "expires={self.expires!r} "
                "filename={self.filename!r} "
                "idle_time={self.idle_time} "
                "ops={self.ops!r} "
                "size={self.size!r} "
                "sparse={self.sparse!r} "
                "transfer_id={self.transfer_id!r} "
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


def _required(d, key, type):
    if key not in d:
        raise errors.MissingTicketParameter(key)
    return _validate(key, d[key], type)


def _optional(d, key, type, default=None):
    if key not in d:
        return default
    return _validate(key, d[key], type)


def _validate(key, value, type):
    if not isinstance(value, type):
        raise errors.InvalidTicketParameter(
            key, value, "expecting a {!r} value".format(type))
    return value


def add(ticket_dict):
    """
    Add a ticket to the store.

    Raises errors.InvalidTicket if ticket dict is invalid.
    """
    ticket = Ticket(ticket_dict)
    _tickets[ticket.uuid] = ticket


def remove(ticket_id):
    del _tickets[ticket_id]


def clear():
    _tickets.clear()


def get(ticket_id):
    """
    Gets a ticket ID and returns the proper
    Ticket object from the tickets' cache.
    """
    return _tickets[ticket_id]


def authorize(ticket_id, op):
    """
    Authorizing a ticket operation
    """
    log.debug("AUTH op=%s ticket=%s", op, ticket_id)
    try:
        ticket = _tickets[ticket_id]
    except KeyError:
        raise errors.AuthorizationError("No such ticket {}".format(ticket_id))

    if ticket.expires <= util.monotonic_time():
        raise errors.AuthorizationError("Ticket {} expired".format(ticket_id))

    if not ticket.may(op):
        raise errors.AuthorizationError(
            "Ticket {} forbids {}".format(ticket_id, op))

    return ticket
