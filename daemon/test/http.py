# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
http helpers.
"""

from __future__ import absolute_import
from __future__ import print_function

import errno
import logging

from pprint import pprint

from six.moves import http_client

from ovirt_imageio_common import config
from ovirt_imageio_common import pki
from ovirt_imageio_common import uhttp

log = logging.getLogger("test")


def connection():
    return http_client.HTTPSConnection(
        config.images.host,
        config.images.port,
        pki.key_file(config),
        pki.cert_file(config))


def get(uri, headers=None):
    return request("GET", uri, headers=headers)


def put(uri, body, headers=None):
    return request("PUT", uri, body=body, headers=headers)


def patch(uri, body, headers=None):
    return request("PATCH", uri, body=body, headers=headers)


def options(uri):
    return request("OPTIONS", uri)


def request(method, uri, body=None, headers=None):
    con = connection()
    try:
        con.request(method, uri, body=body, headers=headers or {})
    except EnvironmentError as e:
        if not (e.errno == errno.EPIPE and body):
            raise
        log.warning("Error sending request: %s", e)
    return response(con)


def raw_request(method, uri, body=None, headers=None):
    """
    Use this to send bad requests - this will send only the headers set in
    headers, no attempt is made to create a correct request.
    """
    con = connection()
    con.putrequest(method, uri)
    if headers:
        for name, value in headers.items():
            con.putheader(name, value)
    con.endheaders()

    try:
        if body:
            con.send(body)
    except EnvironmentError as e:
        if e.errno != errno.EPIPE:
            raise
        log.warning("Error sending body: %s", e)

    return response(con)


def local(method, uri, body=None, headers=None):
    return unix_request(
        config.images.socket, method, uri, body=body, headers=headers)


def unix_request(socket, method, uri, body=None, headers=None):
    con = uhttp.UnixHTTPConnection(socket)
    try:
        con.request(method, uri, body=body, headers=headers or {})
    except EnvironmentError as e:
        if not (e.errno == errno.EPIPE and body):
            raise
        log.warning("Error sending request: %s", e)

    return response(con)


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason, res.version))
    pprint(res.getheaders())
    return res
