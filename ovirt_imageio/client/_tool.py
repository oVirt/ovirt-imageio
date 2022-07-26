# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Tool for transferring disk images.
"""

import logging
import os
import signal
import sys

from . import _options
from . import _download

log = logging.getLogger("tool")

terminated = None


def main():
    parser = _options.Parser()
    _download.register(parser)
    args = parser.parse()

    logging.basicConfig(
        filename=args.log_file,
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s (%(threadName)s) [%(name)s] "
               "%(message)s")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        args.command(args)
    except Exception:
        if terminated:
            log.error("Terminated by signal %d", terminated)
            sys.exit(128 + terminated)
        else:
            log.exception("Command failed")
            sys.exit(1)


def _handle_signal(signo, frame):
    global terminated
    if not terminated:
        terminated = signo
        # Terminate qemu-nbd triggering normal cleanup flow.
        os.killpg(0, signo)
