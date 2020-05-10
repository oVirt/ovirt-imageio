# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from distutils.core import setup
from distutils.core import Extension

from ovirt_imageio._internal import version

setup(
    author="oVirt Authors",
    author_email="devel@ovirt.org",
    description="oVirt imageio",
    license="GNU GPLv2+",
    name="ovirt-imageio",
    packages=[
        "ovirt_imageio",
        "ovirt_imageio._internal",
        "ovirt_imageio._internal.backends",
        "ovirt_imageio.client",
    ],
    platforms=["Linux"],
    scripts=["ovirt-imageio"],
    url="https://gerrit.ovirt.org/ovirt-imageio",
    version=version.string,
    ext_modules=[
        Extension(
            "ovirt_imageio/_internal/ioutil",
            sources=["ovirt_imageio/_internal/ioutil.c"]),
    ]
)
