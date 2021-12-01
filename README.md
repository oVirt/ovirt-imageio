# ovirt-imageio

[![CI status](https://github.com/oVirt/ovirt-imageio/actions/workflows/ci.yml/badge.svg)](https://github.com/oVirt/ovirt-imageio/actions)
[![Copr build status](https://copr.fedorainfracloud.org/coprs/nsoffer/ovirt-imageio-preview/package/ovirt-imageio/status_image/last_build.png)](https://copr.fedorainfracloud.org/coprs/nsoffer/ovirt-imageio-preview/package/ovirt-imageio/)

ovirt-imageio enables uploading and downloading of disks and snapshots using HTTPS.

The project provides ovirt-imageio service and imageio client library.

ovirt-imageio service can act in two different roles:

- as a service exposing images over HTTPS, allowing clients to read and write images.

- as a proxy service, allowing clients without access to the host network to read and write images.

imageio client library provides wrapper around REST API provided by ovirt-imageio.
Besides making REST API easy to use, it also provides other features like on-the-fly image
format conversion or support for incremental backup out of the box. It can also be used
as a reference implementation of imageio client.

## How this project is related to other oVirt projects?

- vdsm - control and monitor imageio service, prepare and finalize
  upload and download operations.

- engine - manage upload and download operations, communicating with
  vdsm and imageio service running on the engine. Engine does not communicate with imageio
  service on the hosts.

## Documentation

[http://ovirt.github.io/ovirt-imageio/](http://ovirt.github.io/ovirt-imageio/)

## Installation

- On engine, the imageio service is installed as part of [oVirt engine installation](https://www.ovirt.org/documentation/install-guide/chap-Installing_oVirt/)

    - Should be enabled during engine-setup.

- On host, the imageio service is installed as part of [oVirt node installation](https://www.ovirt.org/node/)

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
