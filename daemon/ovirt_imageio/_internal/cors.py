# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
cors - Cross-Origin Resource Sharing support

Browser send OPTIONS preflight request before sending other request to
check if the CORS protocol is supported, and the server allows the
method and headers.  The result of the preflight request may be cached
by the browser.

Here is an example preflight request sent during upload:

Host: example.com:54323
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; ... Firefox/74.0
Accept: /
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate, br
Access-Control-Request-Method: PUT
Access-Control-Request-Headers: cache-control,content-range,content-type
Referer: https://example.com:8443/ovirt-engine/webadmin/?locale=en_US
Origin: https://example.com:8443
Connection: keep-alive

The response must include these headers:

Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: *
Access-Control-Allow-Methods: OPTIONS,GET,PUT
Access-Control-Max-Age: 86400

In this case we support any origin, any header and methods OPTIONS, GET,
and PUT.

For more info see:
https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS.
"""

import functools


def allow(allow_origin="*", allow_headers="*", allow_methods="*",
          max_age=24 * 3600):
    """
    Returns a decorator adding CORS headers to preflight request (OPTIONS) or
    actual request with a Origin header.

    Used for methods that may be called from engine webadmin.

    Example usage:

        @cors.allow()
        def get(self, req, resp):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, req, resp, *args):
            modified = False

            # Allow only allow_origin.
            if req.headers.get("origin"):
                resp.headers["access-control-allow-origin"] = allow_origin
                modified = True

            # Allow only allow_headers.
            if req.headers.get("access-control-request-headers"):
                resp.headers["access-control-allow-headers"] = allow_headers
                modified = True

            # Allow only allow_methods.
            if req.headers.get("access-control-request-method"):
                resp.headers["access-control-allow-methods"] = allow_methods
                modified = True

            # Clients can cache the response for specified seconds.
            if modified:
                resp.headers["access-control-max-age"] = max_age

            return func(self, req, resp, *args)

        return wrapper

    return decorator
