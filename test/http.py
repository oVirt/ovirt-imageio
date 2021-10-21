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

import errno
import http.client
import logging

from pprint import pprint

from ovirt_imageio._internal import errors
from ovirt_imageio._internal import ssl
from ovirt_imageio._internal import uhttp

log = logging.getLogger("test")


class HTTPClient:

    def __init__(self, con):
        self.con = con

    def get(self, uri, headers=None):
        return self.request("GET", uri, headers=headers)

    def put(self, uri, body, headers=None):
        return self.request("PUT", uri, body=body, headers=headers)

    def delete(self, uri, headers=None):
        return self.request("DELETE", uri, headers=headers)

    def patch(self, uri, body, headers=None):
        return self.request("PATCH", uri, body=body, headers=headers)

    def options(self, uri, headers=None):
        return self.request("OPTIONS", uri, headers=headers)

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


class RemoteClient(HTTPClient):

    def __init__(self, cfg):
        if cfg.tls.enable:
            context = ssl.client_context(cfg.tls.ca_file)
            con = http.client.HTTPSConnection(
                cfg.remote.host,
                cfg.remote.port,
                context=context)
        else:
            con = http.client.HTTPConnection(cfg.remote.host, cfg.remote.port)

        super().__init__(con)


class LocalClient(HTTPClient):

    def __init__(self, cfg):
        super().__init__(uhttp.UnixHTTPConnection(cfg.local.socket))


class ControlClient(HTTPClient):

    def __init__(self, cfg):
        transport = cfg.control.transport.lower()
        if transport == "tcp":
            con = http.client.HTTPConnection("localhost", cfg.control.port)
        elif transport == "unix":
            con = uhttp.UnixHTTPConnection(cfg.control.socket)
        else:
            raise errors.InvalidConfig("control.transport", transport)

        super().__init__(con)


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason, res.version))
    pprint(res.getheaders())
    return res
