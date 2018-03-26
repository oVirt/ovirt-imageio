"""
HTTP test helpers
"""

from __future__ import print_function

import httplib
import json

from contextlib import closing
from pprint import pprint


def patch(proxy_server, path, msg, headers=None):
    body = json.dumps(msg).encode("utf-8")
    if headers is None:
        headers = {}
    headers["Content-Type"] = "application/json"
    return request(proxy_server, "PATCH", path, body=body, headers=headers)


def request(proxy_server, method, uri, body=None, headers=None):
    if headers is None:
        headers = {}

    if proxy_server.use_ssl:
        con = httplib.HTTPSConnection("localhost",
                                      proxy_server.port,
                                      proxy_server.ssl_key_file,
                                      proxy_server.ssl_cert_file,
                                      timeout=3)
    else:
        con = httplib.HTTPConnection("localhost",
                                     proxy_server.port,
                                     timeout=3)
    with closing(con):
        con.request(method, uri, body=body, headers=headers)
        return _response(con)


def _response(con):
    res = con.getresponse()
    pprint((res.status, res.reason))
    pprint(res.getheaders())
    return res
