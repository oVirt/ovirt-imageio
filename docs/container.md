<!--
SPDX-FileCopyrightText: Red Hat, Inc.
SPDX-License-Identifier: GPL-2.0-or-later
-->

# Using the ovirt-imageio container

## Overview

You can use ovirt-imageio to transfer images without oVirt using the
ovirt-imageio container image.

This way, you can run an image container for every transfer,
exposing the container for remote access, and connecting the
relevant storage. When the transfer is done, the user will
unexpose the container and terminate it.

In this mode, instead of having to add a ticket using the control
service, the imageio server will serve a single ticket, automatically
generated in the container when starting.
Actually, the control service is disabled.

To obtain the public container image you can run:

    podman pull quay.io/ovirt/ovirt-imageio

## Run the container

The ovirt-imageio container have an entrypoint script that takes
additional arguments:

```
$ podman run \
        --interactive \
        --tty \
        --rm \
        quay.io/ovirt/ovirt-imageio:latest --help
usage: entrypoint.py [-h] [--ticket-id TICKET_ID] image

ovirt-imageio

positional arguments:
  image                 Path to image.

options:
  -h, --help            show this help message and exit
  --ticket-id TICKET_ID
                        Optional. Set the ID of the ticket.
                        It is recommended to use with random string for better security.
                        Defaults to the image filename.
```

To run the container and serve an image, you need to mount the
image in the container, and pass the full path to the image
inside the container in the command line.

The container script will explore the image properties, create a ticket for
you, serve the image using an NBD server, and expose it through http at
the port 80 using the ovirt-imageio server.

### Example session

Create the image that we want to expose through the
containerized ovirt-imageio:

```
$ mkdir /var/tmp/images
$ qemu-img create -f raw /var/tmp/images/disk.raw 6g
```

Expose the disk at `http://localhost:8080/images/example`:

```
$ podman run \
    --interactive \
    --tty \
    --rm \
    --publish 8080:80 \
    --volume /var/tmp/images:/images:Z \
    quay.io/ovirt/ovirt-imageio:latest --ticket-id example /images/disk.raw
2022-10-08 14:41:14,961 INFO    (MainThread) [server] Starting (hostname=51365cc84a8b pid=1, version=2.4.7)
2022-10-08 14:41:14,966 INFO    (MainThread) [services] remote.service listening on ('::', 80)
2022-10-08 14:41:14,966 INFO    (MainThread) [server] Initial ticket: /app/ticket.json
2022-10-08 14:41:14,967 INFO    (MainThread) [server] Ready for requests
```

In another shell, download the image (e.g., with `curl`):

```
$ curl http://localhost:8080/images/example -o /var/tmp/download.raw
    % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                   Dload  Upload   Total   Spent    Left  Speed
100 6144M  100 6144M    0     0  1342M      0  0:00:04  0:00:04 --:--:-- 1347M

$ ls -lhs /var/tmp/download.raw
6.0G -rw-r--r--. 1 user user 6.0G Nov  3 09:08 download.raw
```

However, curl does not understand sparseness, thus transferring gigabytes
of zeroes through the network, and creating fully allocated images.

Using a smarter client that could skip zero extents (e.g., the example
`imageio-client`) will result in faster download speeds, and a sparsified
image downloaded:

```
$ examples/imageio-client download http://localhost:8080/images/example /var/tmp/download.qcow2
[ 100% ] 6.00 GiB, 0.08 s, 79.13 GiB/s

$ ls -lhs /var/tmp/download.qcow2
260K -rw-r--r--. 1 user user 320K Nov  3 09:10 download.qcow2
```

Note that the default behaviour of the imageio-client is transforming the
image to qcow2 format, as it is the best format for transferring images to
other systems.

## Build the image

In most usecases, the image should be obtained from the quay.io repository
to get the latest stable build. However, for developers, it may be interesting
to build a new image for testing. To create the container, run:

    make container

This creates `localhost/ovirt-imageio` container image using podman
configured for [rootless environment](https://github.com/containers/podman/blob/main/docs/tutorials/rootless_tutorial.md).

Next, you can just run it as the previous example.
