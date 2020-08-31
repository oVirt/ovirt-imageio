# ovirt-imageio
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

        string = "value"
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

String values must use unicode literals (e.g. "value"). bytes values (.e.g
b"value" or "value") are not supported.

In case that using python keyword as an option is required, corresponding class
attribute has to have prefix "keyword__", e.g. to load config

    # /etc/app/app.conf

    [foo]
    class = my.app.class

appropriate config class should be

    # config.py

    class foo:

        keyword__class = "default.class"
"""

import configparser
import keyword

KEYWORD_PREFIX = "keyword__"


def keyword_mapping(option):
    option = option.lower()
    if keyword.iskeyword(option):
        option = "{}{}".format(KEYWORD_PREFIX, option)
    return option


def load(config, files):
    parser = configparser.RawConfigParser()
    parser.optionxform = keyword_mapping
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


def to_dict(config):
    cfg_dict = {}
    for section_name in _public_names(config):
        section = getattr(config, section_name)
        cfg_dict[section_name] = _obj_to_dict(section)
    return cfg_dict


def _public_names(obj):
    return [name for name in dir(obj) if not name.startswith("_")]


def _obj_to_dict(obj):
    obj_dict = {}
    for attr in _public_names(obj):
        value = getattr(obj, attr)
        if attr.startswith(KEYWORD_PREFIX):
            attr = attr[len(KEYWORD_PREFIX):]
        obj_dict[attr] = value
    return obj_dict


def _validate_bool(s):
    # Use the same values configparser accepts
    val = s.lower()
    if val in ("true", "yes", "1", "on"):
        return True
    elif val in ("false", "no", "0", "off"):
        return False
    raise ValueError("Invalid boolean value: %r" % s)


_validators = {
    str: str,
    int: int,
    float: float,
    bool: _validate_bool,
}
