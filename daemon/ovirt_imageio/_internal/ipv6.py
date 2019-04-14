# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
ipv6 - IP v6 helper module
"""


def unquote_address(host):
    """
    Unquote IPv6 quoted numeric address.
    """
    if host.startswith("[") and host.endswith("]") and ":" in host:
        host = host[1:-1]
    return host


def quote_address(host):
    """
    Quote IPv6 numeric address.
    """
    if ":" in host:
        host = "[{}]".format(host)
    return host
