# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

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
