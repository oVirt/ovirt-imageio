# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from distutils.core import setup
from distutils.core import Extension

from ovirt_imageio import version

setup(
    author="Nir Soffer, Greg Padgett, Amit Aviram, Daniel Erez",
    author_email="nsoffer@redhat.com, gpadgett@redhat.com, "
                 "aaviram@redhat.com, derez@redhat.com",
    description="oVirt imageio",
    license="GNU GPLv2+",
    name="ovirt-imageio",
    packages=[
        "ovirt_imageio",
        "ovirt_imageio.backends"
    ],
    platforms=["Linux"],
    scripts=["ovirt-imageio"],
    url="https://gerrit.ovirt.org/ovirt-imageio",
    version=version.string,
    ext_modules=[
        Extension(
            "ovirt_imageio/ioutil",
            sources=["ovirt_imageio/ioutil.c"]),
    ]
)
