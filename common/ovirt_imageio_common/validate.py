# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

from webob.exc import HTTPBadRequest


def enum(d, name, values):
    if name not in d:
        raise HTTPBadRequest("Missing required value for %r" % name)
    val = d[name]
    if val not in values:
        raise HTTPBadRequest("Unsupported value %r for %r, expecting %s"
                             % (val, name, values))
    return val


def integer(d, name, minval=None, maxval=None, default=None):
    try:
        val = d[name]
    except KeyError:
        if default is not None:
            return default
        raise HTTPBadRequest("Missing required value for %r" % name)
    if not isinstance(val, int):
        raise HTTPBadRequest("Integer required %r" % val)
    if minval is not None and val < minval:
        raise HTTPBadRequest("Invalid value %d < %d" % (val, minval))
    if maxval is not None and val > maxval:
        raise HTTPBadRequest("Invalid value %d > %d" % (val, maxval))
    return val


def boolean(d, name, default=False):
    try:
        val = d[name]
    except KeyError:
        if default is not None:
            return default
        raise HTTPBadRequest("Missing required value for %r" % name)
    if not isinstance(val, bool):
        raise HTTPBadRequest("Boolean required %r" % val)
    return val
