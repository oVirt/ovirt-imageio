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

from ovirt_imageio_common.ssl import check_protocol

from ovirt_imageio_proxy import config
from ovirt_imageio_proxy import server

TEST_DIR = os.path.dirname(__file__)


@contextmanager
def run_server(config_file):
    config.load(os.path.join(TEST_DIR, config_file))
    s = server.Server()
    s.start(config)
    try:
        yield s
    finally:
        s.stop()


@pytest.mark.parametrize("protocol", ["-ssl2", "-ssl3", "-tls1", "-tls1_1"])
def test_default_reject(protocol):
    with run_server("proxy.conf") as proxy_server:
        rc = check_protocol("127.0.0.1", proxy_server.port, protocol)
    assert rc != 0


@pytest.mark.parametrize("protocol", ["-tls1_2"])
def test_default_accept(proxy_server, protocol):
    with run_server("proxy.conf") as proxy_server:
        rc = check_protocol("127.0.0.1", proxy_server.port, protocol)
    assert rc == 0


@pytest.mark.parametrize("protocol", ["-ssl2", "-ssl3", "-tls1"])
def test_legacy_reject(protocol):
    with run_server("proxy-tls1_1.conf") as proxy_server:
        rc = check_protocol("127.0.0.1", proxy_server.port, protocol)
    assert rc != 0


@pytest.mark.parametrize("protocol", ["-tls1_1", "-tls1_2"])
def test_legacy_accept(proxy_server, protocol):
    with run_server("proxy-tls1_1.conf") as proxy_server:
        rc = check_protocol("127.0.0.1", proxy_server.port, protocol)
    assert rc == 0
