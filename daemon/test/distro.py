# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import


def is_fedora(version=""):
    return "Fedora release {}".format(version) in redhat_release()


def is_centos(version=""):
    return "CentOS Linux release {}".format(version) in redhat_release()


def redhat_release():
    with open("/etc/redhat-release") as f:
        return f.readline()
