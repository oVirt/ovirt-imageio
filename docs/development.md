# Development

During development you may want to build/install the proxy.  There are
a few ways to go about this:

  `ovirt-imageio-proxy [options]` - run the proxy in-place, with debug
   options and alternate paths to configuration files, if desired.

  `python setup.py install` - Build/install the lib files and main
   script, but not the service/log/etc files.

  `make install` - Build/install everything (with DESTDIR specified
   if packaging); this uses python-setuptools for some of the work.

  `make rpm` - Build the proxy and setup rpms; this uses
   python-setuptools for some of the work.

When using an editor with code completion, it may be useful to have run
`make generated-files` to create any files with constants that depend
on the environment.

Before submitting changes, please be sure to:

 - Apply the git commit template
   (`git config commit.template commit-template.txt`).

 - Verify that unit tests pass (`make check` or `py.test`).  As of this
   writing, running the unit tests on Fedora 23 requires the pytest and
   python-pytest-cov packages.

Please send any patches to:

  gerrit.ovirt.org/ovirt-imageio


Organizing imports:

Imports should be organized in the following order:

 - Future import group

 - Standard library

 - "from" imports (e.g. from contextlib import closing)

 - "six" imports, for supporting both python 2 and 3

 - 3rd party imports

 - Local imports (using from . import ...)

As a general rule, only modules should be imported, never import names
from modules. The only exception is common imports from the standard
library (e.g. contextlib.closing).
