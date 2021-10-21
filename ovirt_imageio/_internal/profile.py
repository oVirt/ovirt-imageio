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

from . import http
from . import validate

log = logging.getLogger("profile")
lock = threading.Lock()


class Handler:
    """
    Request handler for the /profile/ resource.
    """

    def __init__(self, config, auth):
        self.config = config
        self.auth = auth

    def post(self, req, resp):
        """
        Start of stop the profiler.
        """
        if yappi is None:
            raise http.Error(http.NOT_FOUND, "yappi is not installed")

        run = validate.enum(req.query, "run", ("y", "n"))
        if run == "y":
            clock = validate.enum(
                req.query, "clock", ("cpu", "wall"), default="cpu")
            self._start_profiling(clock)
        else:
            self._stop_profiling()

    def get(self, req, resp):
        if yappi is None:
            raise http.Error(http.NOT_FOUND, "yappi is not installed")

        msg = {"running": yappi.is_running()}
        resp.send_json(msg)

    def _start_profiling(self, clock):
        with lock:
            if yappi.is_running():
                raise http.Error(
                    http.BAD_REQUEST, "profile is already running")

            log.info("Starting profiling using %r clock", clock)
            yappi.set_clock_type(clock)
            yappi.start(builtins=True, profile_threads=True)

    def _stop_profiling(self):
        with lock:
            if not yappi.is_running():
                raise http.Error(http.BAD_REQUEST, "profile is not running")

            log.info("Stopping profiling, writing profile to %r",
                     self.config.profile.filename)
            yappi.stop()
            stats = yappi.get_func_stats()
            stats.save(self.config.profile.filename, type="pstat")
            yappi.clear_stats()
