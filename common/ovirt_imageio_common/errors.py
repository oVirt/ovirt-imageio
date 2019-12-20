# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import


class Error(Exception):
    msg = "Overide this in a subclass"

    def __str__(self):
        return self.msg.format(self=self)


class PartialContent(Error):
    msg = "Requested {self.requested} bytes, available {self.available} bytes"

    def __init__(self, requested, available):
        self.requested = requested
        self.available = available


class InvalidTicket(Error):
    """Base class for ticket errors"""


class MissingTicketParameter(InvalidTicket):
    msg = "Required ticket parameter is missing: {self.parameter}"

    def __init__(self, parameter):
        self.parameter = parameter


class InvalidTicketParameter(InvalidTicket):
    msg = ("Invalid value for {self.parameter!r}: "
           "{self.value!r}: {self.reason}")

    def __init__(self, parameter, value, reason):
        self.parameter = parameter
        self.value = value
        self.reason = reason


class AuthorizationError(Error):
    msg = "You are not allowed to access this resource: {self.reason}"

    def __init__(self, reason):
        self.reason = reason


class UnsupportedOperation(Error):
    msg = "Operation not supported: {self.reason}"

    def __init__(self, reason):
        self.reason = reason
