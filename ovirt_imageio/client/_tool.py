# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Tool for transferring disk images.
"""

import logging
import signal
import sys

from . import _app
from . import _download
from . import _options
from . import _upload

log = logging.getLogger("tool")


def main():
    parser = _options.Parser()
    _download.register(parser)
    _upload.register(parser)
    args = parser.parse()

    logging.basicConfig(
        filename=args.log_file,
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s (%(threadName)s) [%(name)s] "
               "%(message)s")

    _app.setup_signals()

    try:
        args.command(args)
    except _app.TerminatedBySignal as e:
        # SIGINT is likey result of Control+C in the shell.
        level = logging.INFO if e.signal == signal.SIGINT else logging.ERROR
        log.log(level, "Exiting: %s", e)
        sys.exit(128 + e.signal)
    except Exception:
        log.exception("Command failed")
        sys.exit(1)
