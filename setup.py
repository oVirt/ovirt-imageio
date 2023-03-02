# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

from setuptools import setup
from setuptools import Extension

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
    scripts=["ovirt-imageio", "ovirt-imageioctl", "ovirt-img"],
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
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Utilities",
    ],
    ext_modules=[
        Extension(
            "ovirt_imageio/_internal/ioutil",
            sources=["ovirt_imageio/_internal/ioutil.c"]),
    ]
)
