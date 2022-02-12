# ovirt-imageio
# Copyright (C) 2021 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
imageio admin library.
"""

import http.client
import json

from .. _internal import uhttp


class Error(Exception):
    """
    Base class for admin library errors.
    """


class ClientError(Error):
    """
    Client failed to communicate with the server.
    """


class ServerError(Error):
    """
    Server request failed.
    """
    def __init__(self, code, message):
        # The HTTP status returned by the server.
        self.code = code

        # The HTTP error message retrned by the server.
        self.message = message

    def __str__(self):
        return f"Server error: {self.code} {self.message}"


class Client:
    """
    imageio admin client.
    """

    def __init__(self, cfg, timeout=60):
        """
        Create a client using specificed configuration.

        Arguments:
          cfg (dict): Server configuration, loaded using admin.load_config().
          timeout (float): Connection timeout in seconds (default 60).
        """
        transport = cfg.control.transport.lower()
        if transport == "tcp":
            self.con = http.client.HTTPConnection(
                "localhost", cfg.control.port, timeout=timeout)
        elif transport == "unix":
            self.con = uhttp.UnixHTTPConnection(
                cfg.control.socket, timeout=timeout)
        else:
            raise ValueError(f"Invalid control.transport: {transport}")

    def add_ticket(self, ticket):
        """
        Add a ticket to imageio daemon.

        Arguments:
          ticket (dict): Ticket to add to running imageio daemon.
        """
        status, body = self._request(
            "PUT",
            f"/tickets/{ticket['uuid']}",
            body=json.dumps(ticket).encode("utf-8"))
        if status != http.client.OK:
            raise ServerError(status, body)

    def get_ticket(self, ticket_id):
        """
        Get ticket information from imageio daemon.

        Arguments:
          ticket_id (str): Ticket id.
        """
        status, body = self._request("GET", f"/tickets/{ticket_id}")
        if status != http.client.OK:
            raise ServerError(status, body)
        return json.loads(body)

    def mod_ticket(self, ticket_id, changes):
        """
        Modify a ticket in imageio daemon.

        Currenlty the only property that may be changed is "timeout".

        Arguments:
          ticket_id (str): Ticket id.
          changes (dict): Ticket properties to modify.
        """
        status, body = self._request(
            "PATCH",
            f"/tickets/{ticket_id}",
            body=json.dumps(changes).encode("utf-8"))
        if status != http.client.OK:
            raise ServerError(status, body)

    def del_ticket(self, ticket_id):
        """
        Delete a ticket from imageio daemon.

        Arguments:
          ticket_id (str): Ticket id.
        """
        status, body = self._request("DELETE", f"/tickets/{ticket_id}")
        if status != http.client.NO_CONTENT:
            raise ServerError(status, body)

    def start_profile(self):
        status, body = self._request("POST", "/profile/?run=y")
        if status != http.client.OK:
            raise ServerError(status, body)

    def stop_profile(self):
        status, body = self._request("POST", "/profile/?run=n")
        if status != http.client.OK:
            raise ServerError(status, body)

    def close(self):
        """
        Close the conection to imageio daemon.
        """
        self.con.close()

    def _request(self, method, uri, body=None):
        try:
            self.con.request(method, uri, body=body)
            res = self.con.getresponse()
            body = res.read().decode("utf-8")
            return res.status, body
        except Exception as e:
            raise ClientError(str(e))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
