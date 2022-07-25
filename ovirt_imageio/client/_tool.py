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

from . import _options
from . import _download


def main():
    parser = _options.Parser()
    _download.register(parser)
    args = parser.parse()

    logging.basicConfig(
        filename=args.log_file,
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s (%(threadName)s) [%(name)s] "
               "%(message)s")

    # XXX Setup signal handlers.
    # XXX Handle commands errors.

    args.command(args)
