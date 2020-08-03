# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import grp
import json
import os
import pwd
import subprocess

import pytest

from contextlib import contextmanager

from ovirt_imageio._internal import config
from ovirt_imageio._internal import server
from ovirt_imageio._internal import sockutil

from . import http

DAEMON_CONFIG = """\
[daemon]
poll_interval = 0.1
buffer_size = 131072
run_dir = {run_dir}
drop_privileges = {drop_priv}
user_name = {user_name}
group_name = {group_name}

[tls]
key_file = test/pki/system/key.pem
cert_file = test/pki/system/cert.pem
ca_file = test/pki/system/ca.pem

[remote]
port = 0

[local]
socket =

[control]
transport = unix
socket = {run_dir}/sock

[logger_root]
level=DEBUG

[handler_logfile]
args=('{log_dir}/daemon.log',)
"""

requires_root = pytest.mark.skipif(os.geteuid() != 0, reason="Requires root")
requires_unprivileged = pytest.mark.skipif(
    os.geteuid() == 0, reason="Requires unprivileged user")


@pytest.fixture
def tmp_dirs(tmpdir):
    tmpdir.mkdir("run")
    tmpdir.mkdir("log")
    tmpdir.mkdir("conf").mkdir("conf.d")


def test_find_configs_same_dir(tmpdir):
    # Config files are sorted by filename in alphabetical increasing order.
    # In this scenario we use install config installed by application and admin
    # config, which will overwrite some settings.
    # Admin configuration typically use higher prefix to override installation
    # config.

    conf_d = tmpdir.mkdir("conf.d")
    install_cfg = conf_d.join("50-install.conf")
    admin_cfg = conf_d.join("90-admin.conf")
    install_cfg.write("install config")
    admin_cfg.write("admin config")

    files = server.find_configs((str(tmpdir),))
    assert files == [str(install_cfg), str(admin_cfg)]


def test_find_configs_multiple_dirs(tmpdir):
    # Same scenario as in test_find_configs_same_dir, but configs are stored
    # in multiple directories. The list of configs provided by find_configs()
    # has to be sorted according to file name. Name of the directories or the
    # order in which the dirs are passed to find_configs() function doesn't
    # matter.

    etc_dir = tmpdir.mkdir("etc")
    etc_conf_d = etc_dir.mkdir("conf.d")
    install_cfg = etc_conf_d.join("50-install.conf")
    install_cfg.write("install config")
    admin_cfg = etc_conf_d.join("90-admin.conf")
    admin_cfg.write("admin config")

    vendor_dir = tmpdir.mkdir("vendor")
    vendor_cfg = vendor_dir.mkdir("conf.d").join("60-vendor.conf")
    vendor_cfg.write("vendor config")

    expected = [str(install_cfg), str(vendor_cfg), str(admin_cfg)]

    assert server.find_configs((str(etc_dir), str(vendor_dir))) == expected
    assert server.find_configs((str(vendor_dir), str(etc_dir))) == expected


def test_config_overwrite(monkeypatch, tmpdir):
    # Here we test full scenario, when config is loaded from multiple sources
    # and test, that specified options were overwritten as expected.
    # Install config overwrites the default settings, then vendor config should
    # be loaded and overwrite log handler level and finally admin config should
    # be loaded and overwrite setup of control service previously defined by
    # install config.

    install_config = """
[control]
transport = unix
socket = test/daemon.sock

[handler_logfile]
class = logging.StreamHandler
args = ()
kwargs = {}
"""

    vendor_config = """
[handler_logfile]
level = ERROR
"""

    admin_config = """
[control]
transport = tcp
port = 10000
"""

    class FakeArgs():
        def __init__(self, conf_dir):
            self.conf_dir = conf_dir

    etc_dir = tmpdir.mkdir("etc")
    etc_conf_d = etc_dir.mkdir("conf.d")
    install_cfg = etc_conf_d.join("50-install.conf")
    install_cfg.write(install_config)
    admin_cfg = etc_conf_d.join("90-admin.conf")
    admin_cfg.write(admin_config)

    vendor_dir = tmpdir.mkdir("vendor")
    vendor_cfg = vendor_dir.mkdir("conf.d").join("60-vendor.conf")
    vendor_cfg.write(vendor_config)

    monkeypatch.setattr(server, "VENDOR_CONF_DIR", str(vendor_dir))
    cfg = server.load_config(FakeArgs(str(etc_dir)))

    assert cfg.control.transport == "tcp"
    assert cfg.control.port == 10000
    assert cfg.handler_logfile.level == "ERROR"
    assert cfg.handler_logfile.keyword__class == "logging.StreamHandler"


def test_show_config():
    cfg = config.load(["test/conf.d/daemon.conf"])
    out = subprocess.check_output(
        ["./ovirt-imageio", "--conf-dir", "./test", "--show-config"])
    assert json.loads(out) == config.to_dict(cfg)


@requires_unprivileged
def test_unprivileged_user(tmpdir, tmp_dirs):
    with started_imageio(tmpdir) as p:
        uid = os.getuid()
        gid = os.getuid()
        status = process_status(p.pid)
        assert status["uids"] == {uid}
        assert status["gids"] == {gid}
        assert status["groups"] == user_groups(uid)

        # Since we run as unprivileged users the daemon run as the same
        # user and group and no ownership changes are made.
        expected_user = pwd.getpwuid(os.getuid()).pw_name
        expected_group = grp.getgrgid(os.getuid()).gr_name
        assert_ownership(tmpdir, expected_user, expected_group)


@requires_root
def test_drop_privileges_disable(tmpdir, tmp_dirs):
    with started_imageio(tmpdir, drop_privileges="false") as p:
        # Run under root and privileges shouldn't be dropped and ownership of
        # files shouldn't be changed.
        status = process_status(p.pid)
        assert status["uids"] == {0}
        assert status["gids"] == {0}

        # Asserting groups directly to root groups fails on Jenkins as the test
        # probably run there under a mock user which inherits some groups.
        # Therefore we assert that the groups are same as groups of the parent
        # process.
        parent_process_status = process_status(os.getpid())
        assert status["groups"] == parent_process_status["groups"]
        assert_ownership(tmpdir, "root", "root")


@requires_root
def test_drop_privileges(tmpdir, tmp_dirs):
    with started_imageio(tmpdir) as p:
        # Run under root but privileges should be dropped, daemon should run
        # under nobody user and relevant files should be owned by this user.
        status = process_status(p.pid)
        pwnam = pwd.getpwnam("nobody")
        assert status["uids"] == {pwnam.pw_uid}
        assert status["gids"] == {pwnam.pw_gid}
        assert status["groups"] == user_groups("nobody")
        assert_ownership(tmpdir, "nobody", "nobody")


def prepare_config(tmpdir, drop_privileges="true"):
    daemon_conf = DAEMON_CONFIG.format(
        run_dir=os.path.join(tmpdir, "run"),
        log_dir=os.path.join(tmpdir, "log"),
        drop_priv=drop_privileges,
        user_name="nobody",
        group_name="nobody",
    )
    tmpdir.join("conf", "conf.d", "daemon.conf").write(daemon_conf)


@contextmanager
def started_imageio(tmpdir, drop_privileges="true"):
    prepare_config(tmpdir, drop_privileges=drop_privileges)

    conf_dir = tmpdir.join("conf")

    cmd = ["./ovirt-imageio", "--conf-dir", str(conf_dir)]
    proc = subprocess.Popen(cmd)
    try:
        socket = sockutil.UnixAddress(str(tmpdir.join("run", "sock")))
        if not sockutil.wait_for_socket(socket, 10):
            raise RuntimeError("Timeout waiting for {}".format(socket))

        # Wait until server is listening - at this point it already dropped
        # privileges.
        if drop_privileges:
            cfg = config.load(str(conf_dir.join("conf.d", "daemon.conf")))
            with http.ControlClient(cfg) as c:
                r = c.get("/tickets/no-such-ticket")
                r.read()
                assert r.status == 404

        yield proc
    finally:
        proc.terminate()


def user_groups(user):
    """
    Return group ids for username (str) or user id (int)
    """
    gids = subprocess.check_output(["id", "--groups", "-z", str(user)]).decode(
        "utf-8").strip("\0").split("\0")
    return {int(gid) for gid in gids}


def parse_ids(line):
    _, values = line.split(":", 1)
    return set(int(x) for x in values.strip().split())


def process_status(pid):
    """
    Returns uid, gid and groups of the process.
    Example for the the process with following status:

        $ cat /proc/2769/status | egrep '^(Uid:|Gid:|Groups:)'
        Uid:    993     993     993     993
        Gid:    990     990     990     990
        Groups: 990

    the output is:

        > process_status(2769)
        {'uids': {993}, 'gids': {990}, 'groups': {990}}
    """
    status_path = os.path.join("/proc", str(pid), "status")
    with open(status_path, "r") as f:
        status_lines = f.readlines()

    for line in status_lines:
        if line.startswith("Uid:"):
            uids = parse_ids(line)
        if line.startswith("Gid:"):
            gids = parse_ids(line)
        if line.startswith("Groups:"):
            groups = parse_ids(line)

    return {"uids": uids, "gids": gids, "groups": groups}


def assert_path_owner(path, expected_user, expected_group):
    path_stat = os.stat(path)
    uid = path_stat.st_uid
    gid = path_stat.st_gid
    user = pwd.getpwuid(uid).pw_name
    group = grp.getgrgid(gid).gr_name

    assert expected_user == user
    assert expected_group == group


def assert_ownership(tmpdir, user, group):
    run_dir = os.path.join(tmpdir, "run")
    assert_path_owner(run_dir, user, group)
    assert_path_owner(str(tmpdir.join("run", "sock")), user, group)
    assert_path_owner(str(tmpdir.join("log", "daemon.log")), user, group)
