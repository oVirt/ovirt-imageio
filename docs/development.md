# Development


## Getting the source

Fork the repository: Navigate to [imageio repository](https://github.com/oVirt/ovirt-imageio)
and click "Fork" button in right upper corner of the page.

Clone the repo from GitHub:

    git clone git@github.com:<your-username>/ovirt-imageio.git

Add [imageio repository](https://github.com/oVirt/ovirt-imageio) as an
upstream repository:

    git remote add upstream git@github.com:oVirt/ovirt-imageio.git

Fetch upstream changes (if you clone right after forking the repo, there
sholdn't be any):

    git pull upstream master


## Setting up development environment

Install the runtime requirements:

    sudo dnf install \
        e2fsprogs \
        gcc \
        git \
        libguestfs-tools-c \
        make \
        openssl \
        python3-devel \
        python3-pip \
        python3-setuptools \
        python3-systemd \
        qemu-img \
        qemu-kvm \
        sudo \
        systemd-udev \
        util-linux \
        xfsprogs

Create a virtual environment for running the tests:

    python3 -m venv ~/venv/ovirt-imageio
    source ~/venv/ovirt-imageio/bin/activate
    pip install --upgrade pip
    pip install --upgrade -r containers/requirements.txt
    deactivate


## Running the tests

Before running the tests, enter the virtual environment:

    source ~/venv/ovirt-imageio/bin/activate

When you are done, you can deactivate the environment using:

    deactivate

Create storage for the tests:

    make storage

See "Creating storage for tests" for more info.

To run all the tests:

    make check

During development, it is recommended to run the tests directly with
tox:

    tox

When working on a specific module, best run the specific module tests
directly using tox:

    tox -e test-py38 test/nbd_test.py

You can also use -k to select only certain tests. Check all available
options using:

    tox -e test-py38 -- --help

To list all test envs use:

    tox -l


## The test images

The test images are created automatically when running "make check". If
you want to create them without running "make check", run:

    make images

To delete the test images run:

    make clean-images


## Using local qemu builds

Some tests run qemu-kvm or qemu-nbd. To run qemu or qemu-nbd built from
source, you can change these environment variables:

    export QEMU=/home/username/src/qemu/build/x86_64-softmmu/qemu-system-x86_64
    export QEMU_NBD=/home/username/src/qemu/build/qemu-nbd

When you run the tests, they will use QEMU and QEMU_NBD from the
environment variables.


## Running the daemon from source

To run the ovirt-imageio daemon directly from source, you need to run
make:

    make

This builds the C extension if needed. Then you can start the daemon:

    ./ovirt-imageio -c test

To daemon is configured to log using DEBUG log level to standard error.
To stop the daemon press Control-C.


## Testing rpms

To build the rpms:

    make clean rpm

To install the built rpms:

    sudo dnf install dist/ovirt-imageio-{daemon,client,common}*.rpm

Once imageio is installed, it is easier to upgrade the installed rpms:

    sudo dnf upgrade dist/*.rpm


## Submitting patches

Create dedicated branch for the issue you are going to work on:

    git checkout -b my_issue

Do the changes and commit them.
Push changes into you github fork

    git push origin my_issue

In the GitHub UI click on the "Compare & pull request" button on the
main repo page, or visit "Pull requests" tab and click on the "New
pull request" button.
Subsequently you can choose against which branch you want to submit pull
request. This is needed when you want to backport some change branch
other than `master` branch.


If you preffer command line, you can check [GitHub CLI](https://cli.github.com/).

## CI

Running tests locally is convenient but before your changes can be
merged, we need to test them on all supported distributions and
architectures.

When you submit a pull request, GitHub actions will run all the tests
on:

- Centos Stream 8, x86_64
- Centos Stream 9, x86_64
- Fedora 34, x86_64
- Fedora 35, x86_64


## Creating storage for tests

The tests use the userstorage tool to create files and block devices
with various sector sizes for testing.

To create storage run:

    make storage

The storage is configured in the file `storage.py` in the root of the
project. Tests that use user storage load this configuration file and
access the storage via the BACKENDS dict.

Some storage may not be available on all environments. For example on
CentOS 7 userstorage cannot create storage with 4k sector size, and in
oVirt CI, this usually fails. Tests should check if storage exists and
mark test as xfail or skip when the storage is not available.

Usually there is no need to delete storage created by "make storage",
however if you want to do this, run:

    make clean-storage

For more info on userstorage see https://github.com/nirs/userstorage.


## Organizing imports:

Imports should be organized in the following groups, separated by one
blank line:

- Future import group (e.g. `from __future__ import ...`)

- Standard library (e.g. `import os`)

- "from" imports (e.g. `from contextlib import closing`)

- 3rd party imports (e.g. `import six`)

- Local imports (e.g. `from . import nbd`)

As a general rule, only modules should be imported, never import names
from modules. The only exception is common imports from the standard
library (e.g. contextlib.closing).
