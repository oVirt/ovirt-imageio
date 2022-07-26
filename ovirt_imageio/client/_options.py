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

from .. _internal.units import KiB, MiB, GiB, TiB

import argparse
import configparser
import os
import sys


class Parser:

    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description="Transfer disk images")
        # XXX password should not be here.
        self._parser.set_defaults(command=None, password=None)
        self._commands = self._parser.add_subparsers(title="commands")

    def add_sub_command(self, name, help, func):
        cmd = self._commands.add_parser(name, help=help)
        cmd.set_defaults(command=func)

        cmd.add_argument(
            "-c", "--config",
            help="If set, read specified section from the configuration "
                 f"file ({self.config_file}).")

        cmd.add_argument(
            "--engine-url",
            help="ovirt-engine URL. If not set, read from the specified "
                 "config section (required).")

        cmd.add_argument(
            "--username",
            help="ovirt-engine username. If not set, read from the specified "
                 "config section (required).")

        cmd.add_argument(
            "--password-file",
            help="Read ovirt-engine password from file. If not set, read "
                 "password from the specified config section.")

        cmd.add_argument(
            "--cafile",
            help="Path to ovirt-engine CA certificate. If not set, read from "
                 "the specified config section")

        return cmd

    def parse(self, args=None):
        args = self._parser.parse_args(args=args)
        if not args.command:
            self._parser.print_help()
            sys.exit(2)

        if args.config:
            self._merge_config(args)

        for name in ("engine_url", "username"):
            if getattr(args, name, None) is None:
                self._parser.error(f"Option '{name}' is required")

        return args

    @property
    def config_file(self):
        # https://specifications.freedesktop.org/basedir-spec
        base_dir = os.environ.get("XDG_CONFIG_HOME")
        if not base_dir:
            base_dir = os.path.expanduser("~/.config")
        return os.path.join(base_dir, "ovirt-img.conf")

    def _merge_config(self, args):
        config = configparser.ConfigParser(interpolation=None)
        config.read([self.config_file])

        if not config.has_section(args.config):
            self._parser.error(
                f"No section: '{args.config}' in '{self.config_file}'")

        for name in ("engine_url", "username", "password", "cafile"):
            if getattr(args, name) is not None:
                continue

            if config.has_option(args.config, name):
                setattr(args, name, config.get(args.config, name))


class SizeValue(int):

    SUFFIXES = {"": 1, "k": KiB, "m": MiB, "g": GiB, "t": TiB}

    def __str__(self):
        n = int(self)
        for unit in self.SUFFIXES:
            if n < KiB:
                break
            n //= KiB
        return f"{n}{unit}"


class Size:
    """
    Convert and validate size string.
    """

    def __init__(self, minimum=0, default=None, maximum=None):
        # Minimum value is required since negative size does not make sense.
        self.minimum = SizeValue(minimum)
        self.default = None if default is None else SizeValue(default)
        self.maximum = None if maximum is None else SizeValue(maximum)

    def __call__(self, s):
        if s == "":
            raise ValueError(f"Invalid size: {s!r}")

        unit = SizeValue.SUFFIXES.get(s[-1].lower())
        if unit:
            value = int(s[:-1]) * unit
        else:
            value = int(s)

        if value < self.minimum:
            raise ValueError(f"Size {s!r} < {self.minimum}")

        if self.maximum is not None and value > self.maximum:
            raise ValueError(f"Size {s!r} > {self.maximum}")

        return value
