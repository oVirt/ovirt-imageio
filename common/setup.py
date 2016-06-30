# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from distutils.core import setup

from ovirt_imageio_common import version

setup(
    author="Nir Soffer, Greg Padgett, Amit Aviram",
    author_email="nsoffer@redhat.com, gpadgett@redhat.com, aaviram@redhat.com",
    description="oVirt imageio common library",
    license="GNU GPLv2+",
    name="ovirt-imageio-common",
    packages=["ovirt_imageio_common"],
    platforms=["Linux"],
    url="https://gerrit.ovirt.org/ovirt-imageio",
    version=version.string,
)
