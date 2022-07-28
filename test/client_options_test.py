# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import getpass
import os
import uuid

import pytest

from ovirt_imageio._internal.units import KiB, MiB, GiB, TiB
from ovirt_imageio.client import _options


@pytest.mark.parametrize("value,size", [
    (42, "42"),
    (256 * KiB, "256k"),
    (32 * MiB, "32m"),
    (10 * GiB, "10g"),
    (1 * TiB, "1t"),
])
def test_sizevalue(value, size):
    sv = _options.SizeValue(value)
    assert sv == value
    assert str(sv) == size


@pytest.mark.parametrize("size,value", [
    ("0", 0),
    ("42", 42),
    ("256k", 256 * KiB),
    ("256K", 256 * KiB),
    ("32m", 32 * MiB),
    ("32M", 32 * MiB),
    ("10g", 10 * GiB),
    ("10G", 10 * GiB),
    ("1t", 1 * TiB),
    ("1T", 1 * TiB),
])
def test_size(size, value):
    validate = _options.Size()
    assert validate(size) == value


def test_size_invalid():
    validate = _options.Size()
    with pytest.raises(ValueError) as e:
        validate("not a number")
    assert repr("not a number") in str(e.value)


def test_size_negative():
    validate = _options.Size()
    with pytest.raises(ValueError) as e:
        validate("-1")
    assert repr("-1") in str(e.value)
    assert str(validate.minimum) in str(e.value)


def test_size_minimum():
    validate = _options.Size(minimum=42)
    assert validate("42") == 42
    with pytest.raises(ValueError) as e:
        validate("41")
    assert repr("41") in str(e.value)
    assert str(validate.minimum) in str(e.value)


def test_size_minimum_none():
    with pytest.raises(TypeError):
        _options.Size(minimum=None)


def test_size_maximum():
    validate = _options.Size(maximum=42)
    assert validate("42") == 42
    with pytest.raises(ValueError) as e:
        validate("43")
    assert repr("43") in str(e.value)
    assert str(validate.maximum) in str(e.value)


def test_size_values():
    validate = _options.Size(
        minimum=4 * KiB,
        default=2 * MiB,
        maximum=1 * GiB)
    assert validate.minimum == 4 * KiB
    assert str(validate.minimum) == "4k"
    assert validate.default == 2 * MiB
    assert str(validate.default) == "2m"
    assert validate.maximum == 1 * GiB
    assert str(validate.maximum) == "1g"


def test_uuid_valid():
    validate = _options.UUID
    id = str(uuid.uuid4())
    assert validate(id) == id


def test_uuid_invalid():
    validate = _options.UUID
    id = "invalid uuid"
    with pytest.raises(ValueError) as e:
        validate(id)
    assert id in str(e.value)
    assert "UUID" in str(e.value)


@pytest.fixture
def config(tmpdir, monkeypatch):
    monkeypatch.setitem(os.environ, "XDG_CONFIG_HOME", str(tmpdir))
    config = tmpdir.join("ovirt-img.conf")
    config.write("""
[all]
engine_url = https://engine.com
username = username
password = password
cafile = /engine.pem
log_file = /var/log/ovirt-img/engine.log
log_level = info

[required]
engine_url = https://engine.com
username = username

[missing1]
engine_url = https://engine.com
#username is not set

[missing2]
#engine_url is not set
username = username

[missing3]
#engine_url is not set
#username is not set
password = password

[invalid]
engine_url = https://engine.com
username = username
password = password
log_level = invalid log level
""")


def test_config_all(config):
    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)
    args = parser.parse(["test", "-c", "all"])
    assert args.engine_url == "https://engine.com"
    assert args.username == "username"
    assert args.cafile == "/engine.pem"
    assert args.log_file == "/var/log/ovirt-img/engine.log"
    assert args.log_level == "info"
    assert args.max_workers == 4
    assert args.buffer_size == 4 * MiB

    # Use password from config.
    assert args.password_file is None
    assert args.password == "password"


def test_config_all_override(config, tmpdir):
    password_file = tmpdir.join("password")
    password_file.write("password")

    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)
    args = parser.parse([
        "test",
        "-c", "all",
        "--engine-url", "https://engine2.com",
        "--username", "username2",
        "--password-file", str(password_file),
        "--cafile", "/engine2.pem",
        "--log-file", "test.log",
        "--log-level", "debug",
    ])
    assert args.engine_url == "https://engine2.com"
    assert args.username == "username2"
    assert args.cafile == "/engine2.pem"
    assert args.log_file == "test.log"
    assert args.log_level == "debug"
    assert args.max_workers == 4
    assert args.buffer_size == 4 * MiB

    # --password-file overrides password from config.
    assert args.password_file == str(password_file)
    assert args.password == "password"


def test_config_required(config, monkeypatch):
    monkeypatch.setattr(getpass, "getpass", lambda: "password")

    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)
    args = parser.parse(["test", "-c", "required"])
    assert args.engine_url == "https://engine.com"
    assert args.username == "username"
    assert args.cafile is None
    assert args.log_file is None
    assert args.log_level == "warning"
    assert args.max_workers == 4
    assert args.buffer_size == 4 * MiB

    # No --password-file or config password: use getpass.getpass().
    assert args.password_file is None
    assert args.password == "password"


def test_config_required_override(config, tmpdir):
    password_file = tmpdir.join("password")
    password_file.write("password")

    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)
    args = parser.parse([
        "test",
        "-c", "required",
        "--password-file", str(password_file),
        "--cafile", "/engine2.pem",
        "--log-file", "test.log",
        "--log-level", "debug",
    ])
    assert args.engine_url == "https://engine.com"
    assert args.username == "username"
    assert args.cafile == "/engine2.pem"
    assert args.log_file == "test.log"
    assert args.log_level == "debug"
    assert args.max_workers == 4
    assert args.buffer_size == 4 * MiB

    # Read password from --password-file.
    assert args.password_file == password_file
    assert args.password == "password"


@pytest.mark.parametrize("name,missing", [
    ("missing1", ["username"]),
    ("missing2", ["engine_url"]),
    ("missing3", ["engine_url", "username"]),
])
def test_config_missing(config, capsys, name, missing):
    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)
    with pytest.raises(SystemExit):
        parser.parse(["test", "-c", name])
    captured = capsys.readouterr()
    for name in missing:
        assert name in captured.err


def test_config_missing_override(config):
    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)
    args = parser.parse([
        "test",
        "-c", "missing3",
        "--engine-url", "https://engine.com",
        "--username", "username",
    ])
    assert args.engine_url == "https://engine.com"
    assert args.username == "username"


def test_config_invalid(config, capsys):
    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)
    with pytest.raises(SystemExit):
        parser.parse(["test", "-c", "invalid"])
    captured = capsys.readouterr()
    assert repr("invalid log level") in captured.err


def test_config_no_section(config, capsys):
    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)

    # Required paremeters are available via the command line, but the specified
    # configuration does not exist. This is likely a user error so fail loudly.
    with pytest.raises(SystemExit):
        parser.parse([
            "test",
            "-c", "nosection",
            "--engine-url", "https://engine.com",
            "--username", "username",
        ])

    captured = capsys.readouterr()
    assert repr("nosection") in captured.err


def test_transfer_options(config):
    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)

    args = parser.parse([
        "test",
        "-c", "all",
        "--max-workers", "8",
        "--buffer-size", "16m"
    ])
    assert args.max_workers == 8
    assert args.buffer_size == 16 * MiB


def test_transfer_options_disabled(config):
    parser = _options.Parser()
    parser.add_sub_command(
        "test", "help", lambda x: None, transfer_options=False)
    args = parser.parse([
        "test",
        "-c", "all",
    ])
    assert not hasattr(args, 'max_workers')
    assert not hasattr(args, 'buffer_size')


def test_auto_help(capsys):
    # Running without arguments is same as running with --help.
    parser = _options.Parser()
    parser.add_sub_command("test", "help", lambda x: None)

    with pytest.raises(SystemExit):
        parser.parse([])
    err1 = capsys.readouterr().err

    with pytest.raises(SystemExit):
        parser.parse(["--help"])
    err2 = capsys.readouterr().err

    assert err1 == err2
