# ovirt-imageio
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

from ovirt_imageio._internal import configloader


@pytest.fixture
def config():
    class config:
        class foo:
            string = "old"
            string_nonascii = "\u05d0"
            string_null = "\u0000"
            integer = 1
            real = 4.0
            boolean = False

        class bar:
            string = "old"
            keyword__class = "bar.class"
    return config


def test_empty(tmpdir, config):
    conf = str(tmpdir.join("conf"))
    with open(conf, "w"):
        pass
    configloader.load(config, [conf])
    assert config.foo.string == "old"
    assert config.foo.string_nonascii == "\u05d0"
    assert config.foo.string_null == "\u0000"
    assert config.foo.integer == 1
    assert config.foo.real == 4.0
    assert config.foo.boolean is False
    assert config.bar.string == "old"


def test_ignore_unknown_section(tmpdir, config):
    data = """
[foo]
string = new

[unknown]
string = new
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    assert config.foo.string == "new"
    assert config.foo.integer == 1
    assert config.foo.real == 4.0
    assert config.foo.boolean is False
    assert config.bar.string == "old"
    assert not hasattr(config, "unknown")


def test_ignore_unknown_option(tmpdir, config):
    data = """
[foo]
string = new
unknown = 3
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    assert config.foo.string == "new"
    assert config.foo.integer == 1
    assert config.foo.real == 4.0
    assert config.foo.boolean is False
    assert config.bar.string == "old"
    assert not hasattr(config.foo, "unknown")


def test_some(tmpdir, config):
    data = """
[foo]
string = new
boolean = true
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    assert config.foo.string == "new"
    assert config.foo.integer == 1
    assert config.foo.real == 4.0
    assert config.foo.boolean is True
    assert config.bar.string == "old"


def test_full(tmpdir, config):
    data = """
[foo]
string = new
integer = 2
real = 4.1
boolean = true

[bar]
string = new
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    assert config.foo.string == "new"
    assert config.foo.integer == 2
    assert config.foo.real == 4.1
    assert config.foo.boolean is True
    assert config.bar.string == "new"


@pytest.mark.parametrize("value", [
    "True", "tRue", "true",
    "Yes", "yeS", "yes",
    "On", "oN", "on",
    "1",
])
def test_true(tmpdir, config, value):
    data = """
[foo]
boolean = %s
""" % value
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    assert config.foo.boolean is True


@pytest.mark.parametrize("value", [
    "False", "faLse", "false",
    "No", "nO", "no",
    "Off", "ofF", "off",
    "0",
])
def test_false(tmpdir, config, value):
    data = """
[foo]
boolean = %s
""" % value
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    assert config.foo.boolean is False


@pytest.mark.parametrize("option", ["integer", "real", "boolean"])
def test_validate(tmpdir, config, option):
    data = """
[foo]
%s = invalid value
""" % option
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    pytest.raises(ValueError, configloader.load, config, [conf])


def test_unicode(tmpdir, config):
    data = """
[foo]
string_nonascii = \u05d0
string_null = \u0000
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "wb") as f:
        f.write(data.encode("utf-8"))
    configloader.load(config, [conf])
    assert config.foo.string_nonascii == "\u05d0"
    assert config.foo.string_null == "\u0000"


def test_unsupported_default_value(tmpdir):

    class config:
        class section:
            value = b"bytes"

    data = """
[section]
value = bar
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)

    with pytest.raises(ValueError) as e:
        configloader.load(config, [conf])

    error = str(e.value)
    assert "section.value" in error
    assert str(type(config.section.value)) in error


def test_keyword_option(tmpdir, config):
    data = """
[bar]
string = new
class = test.class
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    assert config.bar.string == "new"
    assert config.bar.keyword__class == "test.class"


def test_config_to_dict(tmpdir, config):
    data = """
[foo]
string = new
integer = 2
real = 4.1
boolean = true

[bar]
string = new
class = test.class
"""
    conf = str(tmpdir.join("conf"))
    with open(conf, "w") as f:
        f.write(data)
    configloader.load(config, [conf])
    expected = {
        "foo": {
            "string": "new",
            "integer": 2,
            "real": 4.1,
            "boolean": True,
            "string_nonascii": "\u05d0",
            "string_null": "\u0000",
        },
        "bar": {
            "string": "new",
            "class": "test.class"
        },
    }
    assert expected == configloader.to_dict(config)
