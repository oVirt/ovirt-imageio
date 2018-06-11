# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import threading

try:
    import yappi
except ImportError:
    yappi = None

from webob.exc import (
    HTTPBadRequest,
    HTTPNotFound,
)

from ovirt_imageio_common import validate
from ovirt_imageio_common import web

log = logging.getLogger("profile")
lock = threading.Lock()


class Handler(object):
    """
    Request handler for the /profile/ resource.
    """

    def __init__(self, config, request):
        self.config = config
        self.request = request

    def post(self):
        """
        Start of stop the profiler.
        """
        if yappi is None:
            raise HTTPNotFound("yappi is not installed")

        run = validate.enum(self.request.params, "run", ("y", "n"))
        if run == "y":
            clock = validate.enum(
                self.request.params, "clock", ("cpu", "wall"), default="cpu")
            self._start_profiling(clock)
        else:
            self._stop_profiling()
        return web.response()

    def get(self):
        if yappi is None:
            raise HTTPNotFound("yappi is not installed")
        return web.response(
            payload={"running": yappi.is_running()})

    def _start_profiling(self, clock):
        with lock:
            if yappi.is_running():
                raise HTTPBadRequest("profile is already running")
            log.info("Starting profiling using %r clock", clock)
            yappi.set_clock_type(clock)
            yappi.start(builtins=True, profile_threads=True)

    def _stop_profiling(self):
        with lock:
            if not yappi.is_running():
                raise HTTPBadRequest("profile is not running")
            log.info("Stopping profiling, writing profile to %r",
                     self.config.profile.filename)
            yappi.stop()
            stats = yappi.get_func_stats()
            stats.save(self.config.profile.filename, type="pstat")
            yappi.clear_stats()
