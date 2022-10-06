# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import os

from contextlib import contextmanager

import pytest

from ovirt_imageio._internal import auth
from ovirt_imageio._internal import config
from ovirt_imageio._internal import services
from ovirt_imageio._internal.ssl import check_protocol


@contextmanager
def remote_service(config_file):
    path = os.path.join("test/conf", config_file)
    cfg = config.load([path])
    authorizer = auth.Authorizer(cfg)
    s = services.RemoteService(cfg, authorizer)
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


@pytest.mark.parametrize("protocol", ["-tls1_2", "-tls1_3"])
def test_default_accept(protocol):
    with remote_service("daemon.conf") as service:
        rc = check_protocol("127.0.0.1", service.port, protocol)
    assert rc == 0
