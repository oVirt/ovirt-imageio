# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import pytest

from ovirt_imageio._internal import auth
from ovirt_imageio._internal import config
from ovirt_imageio._internal import errors
from ovirt_imageio._internal import services


@pytest.mark.parametrize("port", [-1, 65536])
def test_invalid_remote_port(port):
    cfg = config.load(["test/conf/daemon.conf"])
    authorizer = auth.Authorizer(cfg)
    cfg.remote.port = port
    with pytest.raises(errors.InvalidConfig):
        services.RemoteService(cfg, authorizer)


@pytest.mark.parametrize("port", [-1, 65536])
def test_invalid_control_port(port):
    cfg = config.load(["test/conf/proxy.conf"])
    authorizer = auth.Authorizer(cfg)
    cfg.control.port = port
    with pytest.raises(errors.InvalidConfig):
        services.ControlService(cfg, authorizer)
