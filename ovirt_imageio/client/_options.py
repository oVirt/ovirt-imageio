# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Tool options.
"""

import getpass
from collections import namedtuple

from .. _internal import version
from .. _internal.units import KiB, MiB, GiB, TiB
from . _api import MAX_WORKERS, BUFFER_SIZE

import argparse
import configparser
import os
import sys
import uuid


ADD_DISK_TIMEOUT = 120
# Possible boolean values in the configuration.
BOOLEAN_VALUES = {'1': True, 'yes': True, 'true': True, 'on': True,
                  '0': False, 'no': False, 'false': False, 'off': False}


class Choices:

    def __init__(self, name, values):
        self.name = name
        self.values = values

    def __call__(self, s):
        if s not in self.values:
            raise ValueError(
                f"Invalid '{self.name}' value: '{s}', choices: {self}")
        return s

    def __str__(self):
        s = ", ".join(self.values)
        return f"{{{s}}}"

    def __repr__(self):
        return repr(self.name)


log_level = Choices("log_level", ("debug", "info", "warning", "error"))
output_format = Choices("output_format", ("text", "json"))


def bool_string(value):
    if value.lower() not in BOOLEAN_VALUES:
        raise ValueError(f"Not a boolean: '{value}'")
    return BOOLEAN_VALUES[value.lower()]


class Option(
        namedtuple(
            "Option", "name,args,config,required,action,type,default,help")):

    __slots__ = ()

    def __new__(cls, name=None, args=(), config=False, required=False,
                action=None, type=str, default=None, help=None):
        """
        Arguments:
            name (str): Option name use in config file or parsed arguments.
            args (List[str]): If set, add command line arguments for this
                option.
            config (bool): If True this option can be set in the config file.
            required (bool): If True this option is required and parsing
                arguments will fail if it is not specified in the command line
                or config file.
            action (Union[argparse.Action, str]): Specify how an argument
                should be handled. Defaults to 'store' action.
            type (callable): Callable converting the command line argument or
                config value to the wanted type. Must raise ValueError if the
                value is invalid and cannot be converted.
            default (Any): The default value if the option was not set in the
                command line arguments or config file.
            help (str): Help message to show in the online help.
        """
        return tuple.__new__(
            cls, (name, args, config, required, action, type, default, help))

    @property
    def kwargs(self):
        option_kwargs = {
            "dest": self.name,
            "default": None,
            "help": self.help,
        }
        if self.action:
            option_kwargs["action"] = self.action
        else:
            option_kwargs["type"] = self.type
        return option_kwargs


class Parser:

    _OPTIONS = [
        Option(
            name="config",
            args=["-c", "--config"],
            help="If set, read specified section from the configuration file.",
        ),
        Option(
            name="engine_url",
            args=["--engine-url"],
            config=True,
            required=True,
            help=("ovirt-engine URL. If not set, read from the specified "
                  "config section (required)."),
        ),
        Option(
            name="username",
            args=["--username"],
            config=True,
            required=True,
            help=("ovirt-engine username. If not set, read from the "
                  "specified config section (required)."),
        ),
        Option(
            name="password",
            config=True,
        ),
        Option(
            name="password_file",
            args=["--password-file"],
            help=("Read ovirt-engine password from file. If not set, read "
                  "password from the specified config section, or prompt "
                  "the user for the password."),
        ),
        Option(
            name="cafile",
            args=["--cafile"],
            config=True,
            help=("Path to ovirt-engine CA certificate. If not set, read "
                  "from the specified config section"),
        ),
        Option(
            name="secure",
            args=["--insecure"],
            action="store_false",
            config=True,
            default=True,
            type=bool_string,
            help=("Do not verify server certificates and host name (not "
                  "recommened).")
        ),
        Option(
            name="output",
            args=["-o", "--output"],
            type=output_format,
            default="text",
            help="Set the format in which the output must be done "
                 f"(choices: {output_format}), default: text).",
        ),
        Option(
            name="disk_timeout",
            config=True,
            type=int,
            default=ADD_DISK_TIMEOUT,
        ),
        Option(
            name="log_file",
            args=["--log-file"],
            config=True,
            help="Log to file instead of stderr.",
        ),
        Option(
            name="log_level",
            args=["--log-level"],
            config=True,
            type=log_level,
            default="warning",
            help=(f"Log level (choices: {log_level}, default: warning)."),
        ),
        Option(
            name="description",
            args=["-d", "--description"],
            config=True,
            required=False,
            type=str,
            default="Uploaded by ovirt-img",
        ),
    ]

    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description="Transfer disk images")
        self._parser.set_defaults(command=None)
        self._parser.add_argument(
            '--version',
            action='version',
            version=f'%(prog)s {version.string}')
        self._commands = self._parser.add_subparsers(title="commands")

    def add_sub_command(self, name, help="help", func=lambda x: None,
                        transfer_options=True):
        cmd = self._commands.add_parser(name, help=help)
        cmd.set_defaults(command=func)

        for option in self._OPTIONS:
            # Skip config only options.
            if not option.args:
                continue

            cmd.add_argument(*option.args, **option.kwargs)

        if transfer_options:
            size = Size(minimum=1, default=MAX_WORKERS, maximum=8)
            cmd.add_argument(
                "--max-workers",
                type=size,
                default=size.default,
                help=f"Maximum number of workers (range: {size.minimum}-"
                f"{size.maximum}, default: {size.default}).")

            size = Size(
                minimum=64 * KiB, default=BUFFER_SIZE, maximum=16 * MiB)
            cmd.add_argument(
                "--buffer-size",
                type=size,
                default=size.default,
                help=f"Buffer size per worker (range: {size.minimum}-"
                f"{size.maximum}, default: {size.default}).")

        return cmd

    def parse(self, args=None):
        args = self._parser.parse_args(args=args)
        if not args.command:
            self._parser.print_help()
            sys.exit(2)

        if args.config:
            self._merge_config(args)

        self._set_defaults(args)
        self._check_required_options(args)
        self._read_password(args)

        return args

    @property
    def config_file(self):
        # https://specifications.freedesktop.org/basedir-spec
        base_dir = os.environ.get("XDG_CONFIG_HOME")
        if not base_dir:
            base_dir = os.path.expanduser("~/.config")
        return os.path.join(base_dir, "ovirt-img.conf")

    def _merge_config(self, args):
        """
        Read options from specified config section and set the value for unused
        command line options. This must be done after parsing command line
        options since we depend on --config.
        """
        config = configparser.ConfigParser(interpolation=None)
        config.read([self.config_file])

        if not config.has_section(args.config):
            self._parser.error(
                f"No section: '{args.config}' in '{self.config_file}'")

        for option in self._OPTIONS:
            # Skip command line only options.
            if not option.config:
                continue

            # Skip options set by the command line.
            if getattr(args, option.name, None) is not None:
                continue

            # Read and validate value from config file.
            if config.has_option(args.config, option.name):
                raw_value = config.get(args.config, option.name)
                try:
                    value = option.type(raw_value)
                except ValueError as e:
                    self._parser.error(e)
                setattr(args, option.name, value)

    def _set_defaults(self, args):
        """
        Add default value for unused command line argument (None by default),
        and unset config only values (attribute missing). This must be done
        after reading config values.
        """
        for option in self._OPTIONS:
            if getattr(args, option.name, None) is None:
                setattr(args, option.name, option.default)

    def _check_required_options(self, args):
        """
        Check that all required options have some value. This must be done
        after setting the defaults.
        """
        missing = [opt.name for opt in self._OPTIONS
                   if opt.required and getattr(args, opt.name) is None]
        if missing:
            self._parser.error(
                f"Missing required options: {', '.join(missing)}")

    def _read_password(self, args):
        """
        Try to read the password from the args.password_file, args.password, or
        prompt the user for the password.
        """
        if args.password_file:
            with open(args.password_file) as f:
                args.password = f.read().rstrip("\n")
        elif args.password is None:
            args.password = getpass.getpass()


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


class Type:
    """
    Wrapper to expose function validators as types.
    """

    def __init__(self, name, func):
        self.name = name
        self.func = func

    def __call__(self, value):
        try:
            return self.func(value)
        except ValueError as e:
            raise ValueError(
                f"Invalid '{self.name}' value: '{value}': {e}") from e

    def __repr__(self):
        return self.name


def _validate_uuid(id):
    return str(uuid.UUID(id))


def _validate_file(filename):
    """
    Validate that the file exists.

    Raises:
        ValueError

    Returns:
        str: filename
    """
    if not os.path.exists(filename):
        raise ValueError(f"{filename} does not exist")
    if not os.path.isfile(filename):
        raise ValueError(f"{filename} is not a file")
    return filename


UUID = Type("UUID", _validate_uuid)
File = Type("File", _validate_file)
