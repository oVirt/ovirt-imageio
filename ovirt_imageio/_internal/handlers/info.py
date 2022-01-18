# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from .. import cors
from .. import version


class Handler:
    """
    Handle requests for the /info resource.
    """

    def __init__(self, config, auth):
        self.config = config
        self.auth = auth

    @cors.allow()
    def get(self, req, resp):
        resp.send_json({"version": version.string})
