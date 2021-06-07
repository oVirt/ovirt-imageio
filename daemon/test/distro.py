# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.


def is_fedora(version=""):
    return _check("Fedora release {}".format(version))


def is_centos(version=""):
    return _check("CentOS Stream release {}".format(version))


def is_rhel(version=""):
    return _check("Red Hat Enterprise Linux release {}".format(version))


def _check(text):
    with open("/etc/redhat-release") as f:
        line = f.readline()
    return text in line
