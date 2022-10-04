# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import logging
import os
import stat
import subprocess
import time
import shutil
from collections import namedtuple
import http.client as http_client

import pytest

from ovirt_imageio._internal.units import KiB

from . import testutil
from . import http


log = logging.getLogger("test")

PODMAN_CMD = "podman"

FILE_SIZE = 16 * KiB
TICKET_ID = "test"
CONTAINER_IMG_PATH = "/images/disk.raw"
CONTAINER_IMAGE = "localhost/ovirt-imageio:latest"

Server = namedtuple("Server", ["host", "port"])


def _imageio_image_missing():
    if shutil.which(PODMAN_CMD) is None:
        return True
    cmd = [PODMAN_CMD, "image", "inspect", CONTAINER_IMAGE]
    try:
        return subprocess.check_call(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        return True


@pytest.fixture
def tmp_image(tmpdir):
    image_path = tmpdir.mkdir("image")
    image = testutil.create_tempfile(
        image_path, name=os.path.basename(CONTAINER_IMG_PATH), size=FILE_SIZE)
    os.chmod(image, stat.S_IROTH | stat.S_IWOTH)
    return str(image)


def _wait_for_server(host, port, timeout):
    start = time.monotonic()
    deadline = start + timeout
    conn = http_client.HTTPConnection(host, port)
    while True:
        try:
            conn.connect()
        except ConnectionRefusedError:
            now = time.monotonic()
            if now >= deadline:
                return False
            time.sleep(0.25)
        else:
            log.debug("Waited for %.6f seconds", time.monotonic() - start)
            return True


@pytest.fixture
def srv(tmp_image):
    host = "localhost"
    random_port = testutil.random_tcp_port()
    cmd = [PODMAN_CMD, "run", "--rm", "-it"]
    # Port redirect.
    cmd.extend(("-p", f"{random_port}:80"))
    # Image volume.
    cmd.extend(("-v", f"{os.path.dirname(tmp_image)}"
                      f":{os.path.dirname(CONTAINER_IMG_PATH)}:Z"))
    cmd.append(CONTAINER_IMAGE)
    cmd.extend(("--ticket-id", TICKET_ID))
    cmd.append(CONTAINER_IMG_PATH)
    # Run command.
    srv_proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for server to start.
    if not _wait_for_server(host, random_port, timeout=10):
        log.error("Dumping server logs:")
        log.warning("%s", srv_proc.stdout.read().decode("utf-8"))
        log.error("%s", srv_proc.stderr.read().decode("utf-8"))
        pytest.fail("Server could not start")
    yield Server(host, random_port)
    srv_proc.terminate()


@pytest.mark.xfail(
    reason="Container image not found",
    strict=True,
    condition=_imageio_image_missing())
def test_containerized_server(srv):
    data = b"a" * (FILE_SIZE // 2) + b"b" * (FILE_SIZE // 2)
    conn = http_client.HTTPConnection(srv.host, srv.port)
    # Test that we can upload.
    # with http.HTTPClient(conn) as c:
    #    res = c.put(f"/images/{TICKET_ID}", data)
    #    assert res.status == http_client.OK
    # Test that we can download and matches the uploaded data.
    with http.HTTPClient(conn) as c:
        res = c.get(f"/images/{TICKET_ID}")
        assert res.read() == data
        assert res.status == http_client.OK
