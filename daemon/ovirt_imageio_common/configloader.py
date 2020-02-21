# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
configloader - simpler configuration loader

This module loads configuration files using ini file format, validates
options and updates given config module.

To load configuration, define the configuration structure in a module,
for example config.py:

    # config.py

    class foo:

        string = u"value"
        integer = 42
        real = 3.14
        boolean = False

    class bar:

        boolean = True

The configuration file should match the class structure:

    # /etc/app/app.conf

    [foo]
    string = new
    integer = 43
    real = 3.15
    boolean = true

    [bar]
    boolean = false

To load the configuration from "/etc/app/app.conf", use:

    import config
    import configloader
    ...
    configloader.load(config, ["/etc/app/app.conf"])
    assert config.foo.integer == 43

Invalid values in the configuration file that do not match the types in
the config module will raise ValueError.

Unknown sections and options in the configuration file are ignored.

String values must use unicode literals (e.g. u"value"). bytes values (.e.g
b"value" or "value") are not supported.
"""

from __future__ import absolute_import

import configparser

from . import util


def load(config, files):
    parser = configparser.RawConfigParser()
    parser.read(files)
    for section_name in _public_names(config):
        section = getattr(config, section_name)
        for option in _public_names(section):
            try:
                value = parser.get(section_name, option)
            except configparser.NoSectionError:
                break
            except configparser.NoOptionError:
                continue

            value_type = type(getattr(section, option))
            if value_type not in _validators:
                raise ValueError(
                    "Unsupported default value type for '{}.{}': {}"
                    .format(section_name, option, value_type))

            validate = _validators[value_type]
            value = validate(value)
            setattr(section, option, value)


def _public_names(obj):
    return [name for name in dir(obj) if not name.startswith("_")]


def _validate_bool(s):
    # Use the same values configparser accepts
    val = s.lower()
    if val in ("true", "yes", "1", "on"):
        return True
    elif val in ("false", "no", "0", "off"):
        return False
    raise ValueError("Invalid boolean value: %r" % s)


_validators = {
    str: util.ensure_text,
    int: int,
    float: float,
    bool: _validate_bool,
}
