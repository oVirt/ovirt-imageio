# vdsm-imaged - vdsm image daemon
# Copyright (C) 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from distutils.core import setup

setup(
    author="Nir Soffer, Greg Padgett, Amit Aviram",
    author_email="nsoffer@redhat.com, gpadgett@redhat.com, aaviram@redhat.com",
    description="vdsm image daemon",
    license="GNU GPLv2+",
    name="vdsm-imaged",
    platforms=["Linux"],
    packages=["imaged"],
    scripts=["vdsm-imaged"],
    url="https://github.com/ovirt/vdsm-imaged",
    version="0.1",
)
