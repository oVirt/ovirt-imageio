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

with open("README.md") as f:
    long_description = f.read()

setup(
    author="oVirt Authors",
    author_email="devel@ovirt.org",
    description="Transfer disk images on oVirt system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="GNU GPLv2+",
    name="ovirt-imageio",
    packages=[
        "ovirt_imageio",
        "ovirt_imageio._internal",
        "ovirt_imageio._internal.backends",
        "ovirt_imageio._internal.handlers",
        "ovirt_imageio.client",
        "ovirt_imageio.admin",
    ],
    platforms=["Linux"],
    scripts=["ovirt-imageio", "ovirt-imageioctl"],
    url="https://github.com/oVirt/ovirt-imageio",
    project_urls={
        "Documentation":
            "http://ovirt.github.io/ovirt-imageio/",
        "Bug Tracker":
            "https://github.com/oVirt/ovirt-imageio/issues",
    },
    version=version.string,
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Developers",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Utilities",
    ],
    ext_modules=[
        Extension(
            "ovirt_imageio/_internal/ioutil",
            sources=["ovirt_imageio/_internal/ioutil.c"]),
    ]
)
