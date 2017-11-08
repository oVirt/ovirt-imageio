"""
shared pytest fixtures
"""

import os
import pytest

from ovirt_imageio_proxy import config
from ovirt_imageio_proxy import server

TEST_DIR = os.path.dirname(__file__)


@pytest.fixture(scope="session")
def proxy_server(request):
    config.load(os.path.join(TEST_DIR, "resources/test_config.ini"))
    server_instance = server.Server()
    server_instance.start(config)
    request.addfinalizer(server_instance.stop)
    return config


@pytest.fixture(scope="session")
def signed_ticket():
    path = os.path.join(TEST_DIR, "resources/auth_ticket.out")
    with open(path, 'r') as f:
        return f.read().rstrip()
