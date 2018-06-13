# ovirt-imageio-proxy
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from ovirt_imageio_common import web

from . import version
from . http_helper import addcors


class RequestHandler(object):
    """
    Request handler for the /info/ resource.
    """

    def __init__(self, config, request, clock=None):
        """
        Arguments:
            config (config object): proxy configuration
            request (webob.Request): underlying http request
        """
        self.config = config
        self.request = request
        self.clock = clock

    @addcors
    def get(self):
        return web.response(payload={'version': version.string})
