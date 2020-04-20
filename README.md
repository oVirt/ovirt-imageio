# ovirt-imageio

[![Build Status](https://travis-ci.org/oVirt/ovirt-imageio.svg?branch=master)](https://travis-ci.org/oVirt/ovirt-imageio)
[![Copr build status](https://copr.fedorainfracloud.org/coprs/nsoffer/ovirt-imageio-preview/package/ovirt-imageio/status_image/last_build.png)](https://copr.fedorainfracloud.org/coprs/nsoffer/ovirt-imageio-preview/package/ovirt-imageio/)

ovirt-imageio enables uploading and downloading of disks and snapshots using HTTPS.

The project is composed of two services:

- Daemon - expose images over HTTPS, allowing clients to read and write images.
  This part is developed in this project.

- Proxy - allowing clients without access to the host network to perform
  I/O disk operations. This part is developed in this project.

## How this project is related to other oVirt projects?

- vdsm - control and monitor imageio daemon, prepare and finalize
  upload and download operations.

- engine - manage upload and download operations, communicating with
  vdsm and imageio proxy.

## Documentation

[http://ovirt.github.io/ovirt-imageio/](http://ovirt.github.io/ovirt-imageio/)

## Installation

- The proxy service is installed as part of [oVirt engine installation](https://www.ovirt.org/documentation/install-guide/chap-Installing_oVirt/)

    - Should be enabled during engine-setup.

- The daemon service is installed as part of [oVirt node installation](https://www.ovirt.org/node/)

## Contributing

Patches are welcome!

- Refer to [Development Section](http://ovirt.github.io/ovirt-imageio/development.html) for build and commit instructions.

- Push patches to [gerrit.ovirt.org:ovirt-imageio](https://gerrit.ovirt.org/#/admin/projects/ovirt-imageio)

    - See [Working with oVirt Gerrit](https://www.ovirt.org/develop/dev-process/working-with-gerrit/) for details.

## Getting Help

Please send mail to one of the following lists for help.

 - For discussion about usage and general help:
   http://lists.ovirt.org/mailman/listinfo/users

 - For technical discussion about the project and its code:
   http://lists.ovirt.org/mailman/listinfo/devel

## Licensing

The project is provided under the terms of the GPLv2 License.
The ovirt-imageio-proxy-setup plugin is provided under the terms of the
Apache License, Version 2.0.

Please see the COPYING files in the proxy and proxy/setup directories
for complete license terms.
