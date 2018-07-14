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

from ovirt_imageio import ssl
from ovirt_imageio import uhttp

log = logging.getLogger("test")


class Client:

    def __init__(self, cfg):
        context = ssl.client_context(
            cfg.tls.cert_file,
            cfg.tls.cert_file,
            cfg.tls.key_file)
        self.con = http_client.HTTPSConnection(
            cfg.remote.host,
            cfg.remote.port,
            context=context)

    def get(self, uri, headers=None):
        return self.request("GET", uri, headers=headers)

    def put(self, uri, body, headers=None):
        return self.request("PUT", uri, body=body, headers=headers)

    def patch(self, uri, body, headers=None):
        return self.request("PATCH", uri, body=body, headers=headers)

    def options(self, uri):
        return self.request("OPTIONS", uri)

    def request(self, method, uri, body=None, headers=None):
        try:
            self.con.request(method, uri, body=body, headers=headers or {})
        except EnvironmentError as e:
            if not (e.errno == errno.EPIPE and body):
                raise
            log.warning("Error sending request: %s", e)
        return response(self.con)

    def raw_request(self, method, uri, body=None, headers=None):
        """
        Use this to send bad requests - this will send only the headers set in
        headers, no attempt is made to create a correct request.
        """
        self.con.putrequest(method, uri)
        if headers:
            for name, value in headers.items():
                self.con.putheader(name, value)
        self.con.endheaders()

        try:
            if body:
                self.con.send(body)
        except EnvironmentError as e:
            if e.errno != errno.EPIPE:
                raise
            log.warning("Error sending body: %s", e)

        return response(self.con)

    def close(self):
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class UnixClient:

    def __init__(self, socket):
        self.con = uhttp.UnixHTTPConnection(socket)

    def request(self, method, uri, body=None, headers=None):
        try:
            self.con.request(method, uri, body=body, headers=headers or {})
        except EnvironmentError as e:
            if not (e.errno == errno.EPIPE and body):
                raise
            log.warning("Error sending request: %s", e)

        return response(self.con)

    def close(self):
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason, res.version))
    pprint(res.getheaders())
    return res
