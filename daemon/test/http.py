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

from contextlib import closing
from contextlib import contextmanager
from pprint import pprint

from six.moves import http_client

from ovirt_imageio_daemon import pki
from ovirt_imageio_daemon import config
from ovirt_imageio_daemon import uhttp


@contextmanager
def connection():
    con = http_client.HTTPSConnection(
        config.images.host,
        config.images.port,
        pki.key_file(config),
        pki.cert_file(config))
    with closing(con):
        yield con


def get(uri, headers=None):
    return request("GET", uri, headers=headers)


def put(uri, body, headers=None):
    return request("PUT", uri, body=body, headers=headers)


def patch(uri, body, headers=None):
    return request("PATCH", uri, body=body, headers=headers)


def options(uri):
    return request("OPTIONS", uri)


def request(method, uri, body=None, headers=None):
    with connection() as con:
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def raw_request(method, uri, body=None, headers=None):
    """
    Use this to send bad requests - this will send only the headers set in
    headers, no attempt is made to create a correct request.
    """
    with connection() as con:
        con.putrequest(method, uri)
        if headers:
            for name, value in headers.items():
                con.putheader(name, value)
        con.endheaders()
        if body:
            con.send(body)
        return response(con)


def unix_request(method, uri, body=None, headers=None):
    con = uhttp.UnixHTTPConnection(config.tickets.socket)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason, res.version))
    pprint(res.getheaders())
    return res
