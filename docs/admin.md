# Administration


## Overview

To control the imageio daemon, use the ovirt-imageioctl command. The
user running the command must be in the `ovirtimg` group to access the
imageio daemon socket (`/run/ovirt-imageio/sock`).

If the imageio daemon is configured to use "tcp" transport, the daemon
can be controlled by any user on the host running the ovirt-imageio
service.

To start serving a disk image via the ovirt-imageio service you need to
add a ticket to the ovirt-imageio daemon, specifying the image location
and how it can be accessed by clients.

Once a ticket was added, clients can access the image via the URL:

    https://example.com:54322/images/ticket-id

The `ticket-id` must be a unique random string such as a UUID type 4.
Pass this URL to clients via a secure channel.

To get information about an installed ticket, you can query the
ovirt-imageio daemon.

To stop serving a disk image, remove the ticket from the imageio daemon.

If the ovirt-imageio service is stopped, all the tickets are removed.

The ovirt-imageio service can also use as a proxy for another host
running the ovirt-imageio service.


## Ticket URL

Disks images re specified by a URL. Here are the supported URLs:

### file

The simplest way to export a local file is a file URL:

    file:///path/to/image

The image can be a regular file or block device, accessible by the
`ovirtimg` user.

The imageio daemon treats all images as raw data when accessing files
directly. If you export a qcow2 image with a backing file, the backing
file is not exported via the imageio daemon, and downloading the image
will return contents of the file, not the guest data in the image.

It recommended to use NBD to enable all features and get best
performance.

### NBD

The imageio daemon support both `unix` and `tcp` NBD URLs:

    nbd:unix:/socket[:exportname=name]
    nbd://server/name

This is the legacy NBD URLs supported by qemu-img and qemu-nbd. The
modern standard NBD URLs are not suported yet.

XXX Link to nbd url spec

### HTTPS

If a client cannot access the host running the ovirt-imageio service,
you can ovirt-imageio service on another host that can be accessed by
the client, and proxy the request to the host.

In this case the ticket URL will be a HTTPS URL on the origin server:

    https://example.com:54322/images/ticket-id

Running as a proxy is support all features but provides reduced
performance.


## Running NBD server

To use a NBD URL, you must run an NBD server. The recommended server is
`qemu-ndb`. You can export any image supported by qemu-nbd, but the
recommended image format are "raw" and "qcow2". Other image formats are
not tested.

You can run the qemu-nbd locally or on remote server. The imageio daemon
does not support yet NBD secure connection.

For most secure connection and best performance, run qemu-nbd on the
same host running the ovirt-imageio service, exporting the image via a
unix socket.

To report holes in images, you must use the --allocation-depth option.
Without this option the imageio daemon will not be able to report holes
in qcow2 images.

### Examples: running qemu-nbd

Example qemu-nbd command to expose a read only image:

    $ qemu-nbd --persistent \
        --read-only \
        --shared=8 \
        --cache=none \
        --aio=native \
        --allocation-depth \
        --socket=/tmp/nbd.sock \
        --format qcow2 \
        fedora-34.qcow2

This command export the image for read only access, allowing up to 8
concurrent connections.

Example qemu-nbd command to expose a writeable image:

    $ qemu-nbd --persistent \
        --shared=8 \
        --cache=none \
        --aio=native \
        --discard=unmap \
        --detect-zeroes=unmap \
        --allocation-depth \
        --socket=/tmp/nbd.sock \
        --format qcow2 \
        fedora-34.qcow2

This command export the image for read and write access, allowing up to 8
concurrent connections writers or readers.

### More info about qemu-nbd

For more info on using exporting images with qemu-nbd, please check the
`vdsm.storage.nbd` module and qemu-nbd manual.

XXX link to vdsm.storage.nbd module and qemu-nbd manual.

## Creating a ticket

XXX document all ticket attributes

Here is an example ticket from the examples directory:

    $ cat examples/nbd.json
    {
      "dirty: true,
      "ops": ["read", "write"]
      "size": 6442450944,
      "sparse": true,
      "timeout": 3000,
      "url": "nbd:unix:/tmp/nbd.sock",
      "uuid": "5519b2b5-804f-467f-854e-03a613e0000e",
    }


## The ovirt_imageio.admin library

XXX Document the class and show example usage.


## The ovirt-imageioctl command

This command can manage tickets used by the ovirt-imageio daemon. It is
installed by the ovirt-imageio-daemon package.

### Adding a ticket

To add the example ticket run:

    $ ovirt-imageioctl add-ticket examples/nbd.json

### Getting ticket information

To get information about the installed ticket run:

    $ ovirt-imageioctl show-ticket 5519b2b5-804f-467f-854e-03a613e0000e
    {
      "active": false,
      "canceled": false,
      "connections": 0,
      "expires": 6338376,
      "idle_time": 1266,
      "ops": [
        "read",
        "write"
      ],
      "size": 6442450944,
      "sparse": true,
      "dirty": true,
      "timeout": 3000,
      "url": "nbd:unix:/tmp/nbd.sock",
      "uuid": "5519b2b5-804f-467f-854e-03a613e0000e"
    }

### Controlling ticket lifetime

If no client access the transfer URL after the ticket timeout, the
ticket will expire, and the client will not be able to access the image.

To extend ticket lifetime, keeping it alive even when on client is
accessing it, you can modify the ticket timeout:

    $ ovirt-imageioctl mod-ticket 5519b2b5-804f-467f-854e-03a613e0000e --timeout 300

This ticket will expire 300 seconds in the future.

To expire a ticket immediately:

    $ ovirt-imageioctl mod-ticket 5519b2b5-804f-467f-854e-03a613e0000e --timeout 0

This ticket cannot be accessed, but you can enable it again by setting a
new timeout.

### Deleting a ticket

To delete a ticket run:

    $ ovirt-imageioctl del-ticket 5519b2b5-804f-467f-854e-03a613e0000e

### Using special configuration directory

When controlling an installed ovirt-imageio daemon the
`ovirt-imageioctl` command read the daemon configuration from the
default location (`/etc/ovirt-imageio`) just like the ovirt-imageio
daemon.

When running the ovirt-imageio daemon using a custom configuration
directory:

    $ ./ovirt-imageio -c examples

You need to use the same configuration directory when running the
`ovirt-imageioctl` command:

    $ ./ovirt-imageioctl -c examples add-ticket examples/nbd.json
