# ovirt-imageio

ovirt-imageio enables uploading and downloading of disks using HTTPS.

The project is composed of two services:

- Daemon - expose images over HTTPS, allowing clients to read and write
  to images. This part is developed in this project.

- Proxy - allowing clients without access to the host network to perform
  I/O disk operations. This part is developed in this project.


## Documentation

[http://ovirt.github.io/ovirt-imageio/](http://ovirt.github.io/ovirt-imageio/)

## Getting Help

Please send mail to one of the following lists for help.

 - For discussion of proxy usage and general help:
   http://lists.ovirt.org/mailman/listinfo/users

 - For technical discussion about the project and its code:
   http://lists.ovirt.org/mailman/listinfo/devel
   
## Contribute

Patches are welcome!

* Refer to [Development Section](docs/development.md) for build and commit instructions. 

* Push patches to [gerrit.ovirt.org:ovirt-imageio](https://gerrit.ovirt.org/#/admin/projects/ovirt-imageio)

    * See [Working with oVirt Gerrit](https://www.ovirt.org/develop/dev-process/working-with-gerrit/) for details.

## Licensing

The oVirt ImageIO Proxy is provided under the terms of the GPLv2 License.
The oVirt ImageIO Proxy setup plugin is provided under the terms of the
Apache License, Version 2.0.

Please see the COPYING files in the proxy and proxy/setup directories
for complete license terms.