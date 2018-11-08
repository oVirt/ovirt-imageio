# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from . import http


def enum(d, name, values, default=None):
    try:
        val = d[name]
    except KeyError:
        if default is not None:
            return default
        raise http.Error(
            http.BAD_REQUEST, "Missing required value for {}".format(name))

    if val not in values:
        raise http.Error(
            http.BAD_REQUEST,
            "Unsupported value {!r} for {!r}, expecting {}"
            .format(val, name, values))

    return val


def integer(d, name, minval=None, maxval=None, default=None):
    try:
        val = d[name]
    except KeyError:
        if default is not None:
            return default
        raise http.Error(
            http.BAD_REQUEST, "Missing required value for {!r}".format(name))

    if not isinstance(val, int):
        raise http.Error(
            http.BAD_REQUEST, "Integer required {!r}".format(val))

    if minval is not None and val < minval:
        raise http.Error(
            http.BAD_REQUEST, "Invalid value {} < {}".format(val, minval))

    if maxval is not None and val > maxval:
        raise http.Error(
            http.BAD_REQUEST, "Invalid value {} > {}".format(val, maxval))

    return val


def boolean(d, name, default=False):
    try:
        val = d[name]
    except KeyError:
        if default is not None:
            return default
        raise http.Error(
            http.BAD_REQUEST, "Missing required value for {!r}".format(name))

    if not isinstance(val, bool):
        raise http.Error(
            http.BAD_REQUEST, "Boolean required {!r}".format(val))

    return val
