# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Tool options.
"""

import argparse


class Parser:

    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description="Transfer disk images")
        self._parser.set_defaults(
            command=lambda _: self._parser.print_help())
        self._commands = self._parser.add_subparsers(title="commands")

    def add_sub_command(self, name, help, func):
        cmd = self._commands.add_parser(name, help=help)
        cmd.set_defaults(command=func)

        cmd.add_argument(
            "--engine-url",
            help="ovirt-engine URL.")

        cmd.add_argument(
            "--username",
            help="ovirt-engine username.")

        cmd.add_argument(
            "--password-file",
            help="Read ovirt-engine password from file.")

        cmd.add_argument(
            "--cafile",
            help="Path to ovirt-engine CA certificate")

        return cmd

    def parse(self):
        args = self._parser.parse_args()
        # XXX Read config file and merge into args.
        return args
