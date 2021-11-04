# Copyright (C) 2021 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import json
import logging
import subprocess
import time

import pytest

from ovirt_imageio._internal import sockutil

from . import testutil

DAEMON_CONF = """\
[daemon]
poll_interval = 0.1
run_dir = {run_dir}
drop_privileges = no

[tls]
key_file = test/pki/system/key.pem
cert_file = test/pki/system/cert.pem
ca_file = test/pki/system/ca.pem

[remote]
port = 0

[local]
socket =

[control]
transport = {control_transport}
socket = {run_dir}/sock
port = {control_port}

[handlers]
keys = stderr

[logger_root]
level = DEBUG
handlers = stderr
"""

log = logging.getLogger("test")


@pytest.fixture(scope="module", params=["unix", "tcp"])
def srv(request, tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp("admin_tool_test")
    conf_file = tmp_dir.mkdir("conf.d").join("daemon.conf")

    random_port = testutil.random_tcp_port()

    conf = DAEMON_CONF.format(
        run_dir=str(tmp_dir),
        control_transport=request.param,
        control_port=random_port)
    conf_file.write(conf)

    log.info("Starting daemon with conf dir %s", tmp_dir)
    cmd = ["./ovirt-imageio", "--conf-dir", str(tmp_dir)]
    proc = subprocess.Popen(cmd)
    try:
        if request.param == "unix":
            socket = sockutil.UnixAddress(str(tmp_dir.join("sock")))
        else:
            socket = sockutil.TCPAddress("localhost", random_port)
        if not sockutil.wait_for_socket(socket, 10):
            raise RuntimeError("Timeout waiting for {}".format(socket))

        log.info("Daemon started with pid %s", proc.pid)
        yield str(tmp_dir)
    finally:
        log.info("Terminating daemon")
        proc.terminate()


def test_ticket_life_cycle(srv):
    log.info("Checking if ticket exits")
    with pytest.raises(subprocess.CalledProcessError) as e:
        run("show-ticket", "-c", srv, "test")
    log.debug("Error: %s", e.value.stderr)

    log.info("Adding ticket")
    run("add-ticket", "-c", srv, "test/ticket.json")

    log.info("Checking if ticket exits")
    out = run("show-ticket", "-c", srv, "test")
    ticket = json.loads(out)
    log.debug("Got ticket: %s", ticket)
    assert ticket["uuid"] == "test"

    log.info("Modifying ticket")
    modification_time = int(time.monotonic())
    run("mod-ticket", "-c", srv, "--timeout", "3000", "test")

    log.info("Checking that ticket was modifed")
    out = run("show-ticket", "-c", srv, "test")
    modified = json.loads(out)
    log.debug("Got ticket: %s", modified)
    assert modified["expires"] >= modification_time + 3000

    log.info("Removing ticket")
    run("del-ticket", "-c", srv, "test")

    log.info("Checking that ticket was removed")
    with pytest.raises(subprocess.CalledProcessError) as e:
        run("show-ticket", "-c", srv, "test")
    log.debug("Error: %s", e.value.stderr)


def run(*args):
    cmd = ["./ovirt-imageioctl"]
    cmd.extend(args)
    log.debug("Running %s", cmd)

    cp = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True)

    return cp.stdout.strip().decode("utf8")
