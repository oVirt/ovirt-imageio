# Development

## Getting the source

Clone from gerrit:

    git clone git://gerrit.ovirt.org/ovirt-imageio

Apply the git commit template:

    git config commit.template commit-template.txt

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

For Fedora 29:

    sudo yum install $(cat automation/check-patch.packages.fc29)

For CentOS 7:

    sudo yum install $(cat automation/check-patch.packages.el7)

Install development requirements:

    yum install git-review

    python -m pip install --user --upgrade tox userstorage

Do not use pip as root unless you like a lot of pain.

Do not install pytest in your development machine. pytest will be
installed by tox in a virtual environment. If you need pytest for other
projects install it in a virtual environment for the other project.


## Running the tests

Before running the tests, make the project:

    make

To run all the tests:

    make check

When working on a specific component, best change to the component
directory and run only the component test:

    cd common
    tox

When working on a specific module, best run the specific module tests
directly using tox:

    tox -e py27 test/foo_test.py

You can also use -k to select only certain tests. Check all available
options using:

    tox -e py27 -- --help


## Submitting patches

To send patches for master branch:

    git review

To send a backport to older branch:

    git review ovit-4.3

To checkout patch from gerrit (e.g. https://gerrit.ovirt.org/c/101904/):

    git review -d 101904


## Installing the proxy with engine development environment

During development you may want to build/install the proxy.  There are
a few ways to go about this:

- `ovirt-imageio-proxy [options]` - run the proxy in-place, with debug
   options and alternate paths to configuration files, if desired.

- `python setup.py install` - Build/install the lib files and main
  script, but not the service/log/etc files.

- `make install` - Build/install everything (with DESTDIR specified
  if packaging); this uses python-setuptools for some of the work.

- `make rpm` - Build the proxy and setup rpms; this uses
  python-setuptools for some of the work.


## General tips

When using an editor with code completion, it may be useful to have run
`make generated-files` to create any files with constants that depend
on the environment.


## Organizing imports:

Imports should be organized in the following groups, separated by one
blank line:

- Future import group (e.g. `from __future__ import ...`)

- Standard library (e..g `import os`)

- "from" imports (e.g. `from contextlib import closing`)

- 3rd party imports (e.g. `import six`)

- Local imports (e.g `from . import ...`)

As a general rule, only modules should be imported, never import names
from modules. The only exception is common imports from the standard
library (e.g. contextlib.closing).
