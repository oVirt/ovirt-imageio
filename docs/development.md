# Development


## Getting the source

Clone from gerrit:

    git clone git://gerrit.ovirt.org/ovirt-imageio

Install the commit message hook:

    wget -P .git/hooks https://gerrit.ovirt.org/tools/hooks/commit-msg

Edit the commit-msg hook to add Signed-off-by header. Add this at the
end of the commit-msg hook:

    # Add Signed-off-by trailer.
    sob=$(git var GIT_AUTHOR_IDENT | sed -n 's/^\(.*>\).*$/Signed-off-by: \1/p')
    git interpret-trailers --in-place --trailer "$sob" "$1"

Make the commit-msg hook executable:

    chmod +x .git/hooks/commit-msg


## Setting up development environment

Install the runtime requirements using automation packages files:

    sudo yum install $(cat automation/check-patch.packages)

Install development tools:

    yum install git-review

Create a virtual environment for running the tests:

    python3 -m venv ~/venv/ovirt-imageio
    source ~/venv/ovirt-imageio/bin/activate
    pip install --upgrade pip
    pip install --upgrade -r docker/requirements.txt
    deactivate


## Running the tests

Before running the tests, enter the virtual environment:

    source ~/venv/ovirt-imageio/bin/activate

When you are done, you can deactivate the environment using:

    deactivate

Create storage for the tests:

    make storage

See "Creating storage for tests" for more info.

Before running the tests, make the project:

    make

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


## Using local qemu builds

Some tests run qemu-kvm or qemu-nbd. To run qemu or qemu-nbd built from
source, you can change these environment variables:

    export QEMU=/home/username/src/qemu/build/x86_64-softmmu/qemu-system-x86_64
    export QEMU_NBD=/home/username/src/qemu/build/qemu-nbd

When you run the tests, they will use QEMU and QEMU_NBD from the
environment variables.


## Testing rpms

To build the rpms:

    make clean rpm

To install the built rpms:

    sudo dnf install dist/ovirt-imageio-{daemon,client,common}*.rpm

Once imageio is installed, it is easier to upgrade the installed rpms:

    sudo dnf upgrade dist/*.rpm


## Submitting patches

To send patches for master branch:

    git review

To send a backport to older branch:

    git review ovit-4.3

To checkout patch from gerrit (e.g. https://gerrit.ovirt.org/c/101904/):

    git review -d 101904


## CI

Running tests locally is convenient but before your changes can be
merged, we need to test them on all supported distributions and
architectures.

When you submit patches to gerrit, oVirt CI will run all the tests on:

- Centos Stream 8, x86_64

Note that tests using 4k storage cannot run in oVirt CI, but they can
run in Travis CI. This is not integrated yet with gerrit, but easy to
run manually.

On Travis CI all tests run on:

- Centos Stream 8, x86_64
- Fedora 32, x86_64
- Fedora 33, x86_64
- Fedora 34, x86_64

To test your changes on Travis:

- Fork the project on github
- Visit https://travis-ci.com, register using your github account, and
  enable builds for your ovirt-imageio fork
- Push your changes to your github fork to trigger a build


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
