#!/usr/bin/python3
#
# ovirt-imageio
# Copyright (C) 2020 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Configure Docker daemon. Has to be run with root privileges.
"""

import json
import subprocess

DAEMON_CONF = "/etc/docker/daemon.json"


def main():
    configure_docker()
    restart_docker_service()


def configure_ipv6(conf):
    """
    Add IPv6 support - enable IPv6 for docker and set IPv6 subnet for fixed
    IPs. The Docker setup is based on
    https://github.com/travis-ci/travis-ci/issues/8891#issuecomment-353403729
    """
    conf["ipv6"] = True
    conf["fixed-cidr-v6"] = "2001:1:1::/64"


def configure_docker():
    """
    Adjust exiting Docker daemon configuration. If there is none, create it.
    """
    try:
        with open(DAEMON_CONF, "r") as f:
            conf = json.loads(f.read())
    except FileNotFoundError:
        conf = {}

    configure_ipv6(conf)

    with open(DAEMON_CONF, "w") as f:
        f.write(json.dumps(conf))


def restart_docker_service():
    cmd = ["systemctl", "restart", "docker.service"]
    subprocess.run(
        cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


if __name__ == "__main__":
    main()
