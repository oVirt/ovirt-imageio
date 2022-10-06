# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

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
