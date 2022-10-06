# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

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
