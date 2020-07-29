# Overview

ovirt-imageio enables uploading and downloading of disks using HTTPS.

The system contains these components:

- [Engine](https://github.com/ovirt/ovirt-engine) - Engine UI starts
  image I/O operations, communicating with Engine backend and
  ovirt-imageio-daemon.  Engine backend communicates with Vdsm on the
  host for preparing for I/O operations, monitoring operations, and
  cleaning up.  This part is developed in the ovirt-engine project.

- [Vdsm](https://github.com/ovirt/vdsm) - prepares a host for image
  I/O operations, provides monitoring APIs for monitoring operations
  progress, and cleans up when the operation is done. Vdsm
  communicates with host's ovirt-imageio-daemon.  This part is
  developed in the vdsm project.

- ovirt-imageio - expose images over HTTPS, allowing clients to read and
  write to images. The ovirt-imageio can also serve as a proxy which
  allows clients without access to the host network to perform I/O disk
  operations.  This part is developed in this project.


## Tickets

Tickets are not persisted. In case of ovirt-imageio-daemon crash or
reboot, Engine will provide a new ticket and possibly point client to
another host to continue the operation.


## Packaging

Multiple packaging options are available:

- `make dist` compile and create a distribution tarball

- `make rpm` compile and create an rpm
