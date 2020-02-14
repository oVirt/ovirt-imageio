# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import os

from contextlib import contextmanager

import pytest

from ovirt_imageio_common import config
from ovirt_imageio_common import configloader
from ovirt_imageio_common import server
from ovirt_imageio_common.ssl import check_protocol

TEST_DIR = os.path.dirname(__file__)


def on_centos(version=""):
    prefix = "CentOS Linux release {}".format(version)
    with open("/etc/redhat-release") as f:
        return f.readline().startswith(prefix)


@contextmanager
def remote_service(config_file):
    path = os.path.join(TEST_DIR, config_file)
    configloader.load(config, [path])
    s = server.RemoteService(config)
    s.start()
    try:
        yield s
    finally:
        s.stop()


@pytest.mark.parametrize("protocol", ["-ssl2", "-ssl3", "-tls1", "-tls1_1"])
def test_default_reject(protocol):
    with remote_service("daemon.conf") as service:
        rc = check_protocol("127.0.0.1", service.port, protocol)
    assert rc != 0


@pytest.mark.parametrize("protocol", ["-tls1_2"])
def test_default_accept(protocol):
    with remote_service("daemon.conf") as service:
        rc = check_protocol("127.0.0.1", service.port, protocol)
    assert rc == 0


@pytest.mark.parametrize("protocol", ["-ssl2", "-ssl3", "-tls1"])
def test_legacy_reject(protocol):
    with remote_service("daemon-tls1_1.conf") as service:
        rc = check_protocol("127.0.0.1", service.port, protocol)
    assert rc != 0


@pytest.mark.parametrize("protocol", [
    pytest.param(
        "-tls1_1",
        marks=pytest.mark.skipif(
            on_centos("8"),
            reason="Default crypto policy disable TLS v1.1"
        )
    ),
    "-tls1_2"
])
def test_legacy_accept(protocol):
    with remote_service("daemon-tls1_1.conf") as service:
        rc = check_protocol("127.0.0.1", service.port, protocol)
    assert rc == 0
