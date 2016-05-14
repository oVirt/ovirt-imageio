# ovirt-imageio-common
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest
from ovirt_image_common import configloader


@pytest.fixture
def config():
    class config:
        class foo:
            string = "old"
            integer = 1
            real = 4.0
            boolean = False
        class bar:
            string = "old"
    return config


def test_empty(tmpdir, config):
    conf = str(tmpdir.join("conf"))
    with open(conf, "w"):
        pass
    configloader.load(config, [conf])
    assert config.foo.string == "old"
    assert config.foo.integer == 1
    assert config.foo.real == 4.0
    assert config.foo.boolean == False
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
    assert config.foo.boolean == False
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
    assert config.foo.boolean == False
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
    assert config.foo.boolean == True
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
    assert config.foo.boolean == True
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
    assert config.foo.boolean == True


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
    assert config.foo.boolean == False


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
