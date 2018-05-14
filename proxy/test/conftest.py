"""
shared pytest fixtures
"""

import os
import collections

import pytest

from ovirt_imageio_proxy import config
from ovirt_imageio_proxy import server

TEST_DIR = os.path.dirname(__file__)


SingedTicket = collections.namedtuple(
    "SingedTicket", "data, id, url")


@pytest.fixture(scope="module")
def proxy_server():
    """
    Run proxy server during the test.
    """
    config.load(os.path.join(TEST_DIR, "resources/test_config.ini"))
    server_instance = server.Server()
    server_instance.start(config)
    try:
        yield config
    finally:
        server_instance.stop()


@pytest.fixture(scope="session")
def signed_ticket():
    path = os.path.join(TEST_DIR, "resources/auth_ticket.out")
    with open(path, 'r') as f:
        data = f.read().rstrip()
    return SingedTicket(
        data,
        "f6fe1b31-1c90-4dc3-a4b9-7b02938c8b41",
        "https://localhost:54322")
