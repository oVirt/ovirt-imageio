# Imageio client library

## Overview

Imageio REST API is small and simple, and HTTP libraries are widely
available, but writing correct and efficient HTTP code is not easy. The
imageio client library makes it easy to use the HTTP API efficiently.

Additionally, imageio client provides advanced features like on-the-fly
image format conversion, efficient upload of sparse images, support for
uploading disk images from OVA files, and improved performance using
multiple connections.

The library provides 2 kinds of APIs:

- High level API: Upload local images or disk images from OVA files, or
  download disk or backups to a local image.

- Low level API: The ImageioClient class provides access to raw guest
  data for disks or backups and supports streaming the data to another
  system, or streaming data from another system to disk.

## The high level API

All the functions in this section work with local files and transfer
URLs. Under the hood, imageio is running qemu-nbd for accessing local
images, and communicating with qemu-nbd using imageio NBD client.

Here is the underlying pipeline used during backup:

    disk -> qemu -> server -> client -> qemu-nbd -> image

When downloading or uploading disks, we use similar pipeline, replacing
qemu with qemu-nbd:

    disk <-> qemu-nbd <-> server <-> client <-> qemu-nbd <-> image

Imageio client is using multiple connections to qemu-nbd and to imageio
server, and the server is using multiple connections to qemu or qemu-nbd
to speed up transfers.

The remote disk and local image can be in any format supported by qemu,
but we test only raw, qcow2, and compressed qcow2. The local image can
be a disk image inside an OVA file.

When the imageio client runs on the hypervisor running the imageio
server serving the image transfer, the client uses HTTP over Unix socket
to communicate with the server for better throughput and lower CPU
usage.

### Getting image info

Before uploading an image, we need to know the image format and virtual
size. This info is available using client.info():

    >>> from ovirt_imageio import client
    >>> client.info("fedora-32.raw")
    {
      'virtual-size': 6442450944,
      'filename': 'fedora-32.raw',
      'format': 'raw',
      'actual-size': 1380061184,
      'dirty-flag': False
    }

    >>> client.info("fedora-32.qcow2")
    {
      "virtual-size": 6442450944,
      "filename": "fedora-32.qcow2",
      "cluster-size": 65536,
      "format": "qcow2",
      "actual-size": 672706560,
      "format-specific": {
        "type": "qcow2",
        "data": {
          "compat": "1.1",
          "compression-type": "zlib",
          "lazy-refcounts": false,
          "refcount-bits": 16,
          "corrupt": false
        }
      },
      "dirty-flag": false
    }

If you used qemu-img command, this output probably looks familiar. Under
the hood, client.info() runs "qemu-img info", the output of qemu-img
info is reported as is.

Getting info works also for images inside OVA archive, You need to
specify the file name inside the archive. For example with this OVA:

    $ tar tvf vm.ova
    -rw-rw-r-- nsoffer/nsoffer   7 2020-10-21 12:37 vm.ovf
    -rw-r--r-- nsoffer/nsoffer 676724736 2020-10-21 12:32 fedora-32.qcow2

We can get all the information needed to upload "fedora-32.qcow2" using:

    >>> client.info("vm.ova", member="fedora-32.qcow2")
    {
      "virtual-size": 6442450944,
      "filename": "json:{\"driver\": \"qcow2\", \"file\": ... }",
      "cluster-size": 65536,
      "format": "qcow2",
      "actual-size": 676737024,
      "format-specific": {
        "type": "qcow2",
        "data": {
          "compat": "1.1",
          "compression-type": "zlib",
          "lazy-refcounts": false,
          "refcount-bits": 16,
          "corrupt": false
        }
      },
      "dirty-flag": false,
      "member-offset": 1536,
      "member-size": 676724736
    }

Note the additional `member-offset` and `member-size` keys. These report
the location of the image inside the OVA. This is used to upload the
image directly from the OVA archive, without extracting the OVA.

When creating a disk in oVirt, use the image `virtual-size` as the
`provisioned_size` of the disk.

### Measuring image required size

When uploading disks to block based storage (FC, iSCSI), you must
allocate enough space for the disk. The required space depends on the
disk format and on the amount of data in the image. If we measure the
same qcow2 compressed image (see section "Getting image info"):

    >>> client.measure("fedora-32.qcow2", "qcow2")
    {
      "bitmaps": 0,
      "required": 1381302272,
      "fully-allocated": 6443696128
    }

We find that we need to allocate 1381302272 bytes for the disk.

Like client.info(), this also works for images inside OVA archive:

    >>> client.measure("vm.ova", "qcow2", member="fedora-32.qcow2")
    {
      "bitmaps": 0,
      "required": 1381302272,
      "fully-allocated": 6443696128,
      "member-offset": 1536,
      "member-size": 676724736
    }

If the image contains dirty bitmaps, the size reported in `bitmaps`
should be added to the `required` size.

When creating a disk in oVirt, use the `required` size as the
`initial_size` of the disk.

### uploading an image

After we inspected and measured an image, and created the destination
disk, we can upload the image by starting an image transfer.

The required argument for upload are:

- `filename`: File name to upload

- `transfer_url`: The `transfer_url` attribute of the image transfer,
  e.g. `https://{imageio.server}:{port}/images/{ticket-id}`.

- `cafile`: The CA certificate used to verify imageio server
   certificate. This is typically the same CA certificate used by
   ovirt-engine. If you are using 3rd party CA certificate for
   ovirt-engine, it will not work with imageio server, so you need to
   the ovirt-engine internal CA used for deploying the hypervisors. On
   an oVirt hypervisor, you can find the CA certificate in
   `/etc/pki/vdsm/certs/cacert.pem`.

Here is an example upload:

    >>> client.upload("fedora-32.qcow2", transfer.transfer_url, cafile)

This call returns when the upload is completed.

Under the hood, this call creates multiple connections to imageio
server, depending on server capabilities, convert the image data to raw
format, and send the raw data to imageio server. On the server side the
image data is converted to the disk format. Unallocated or zeroed areas
in the image are zeroed on the server side efficiently without sending
the zeroes over the wire.

For example, if we upload a qcow2 compressed image to a preallocated
disk on FC storage domain, the image data will converted to raw format
and stored in raw format on the disk.

If you run this code on an oVirt hypervisor that can access the
destination disk, you can configure the image transfer to use the
hypervisor for the image transfer. In this case the upload will optimize
the transfer using Unix socket.

### Displaying transfer progress

To display progress during upload or download, you can use
`client.ProgressBar`:

    >>> from ovirt_imageio.client import ProgressBar
    >>> with ProgressBar() as pb:
    ...     client.upload(
    ...         "fedora-32.qcow2",
    ...         transfer.transfer_url,
    ...         cafile,
    ...         progress=pb)
    ...
    [ 100.00% ] 6.00 GiB, 5.33 seconds, 1.13 GiB/s

The output is written by default to stdout. You can use the `output`
argument to write the output to another file.

    >>> with ProgressBar(output=sys.stderr) as pb:

### downloading disk or backup


### downloading incremental backup


### Dealing with inaccessible hypervisors

If you cannot access the hypervisor from the host running imageio
client, you can perform the transfer via the engine host.

The simplest way deal with this issue is the specify the `proxy_url`
argument using the image transfer `proxy_url`:

    >>> client.upload(
    ...     "fedora-32.qcow2",
    ...     transfer.transfer_url,
    ...     cafile,
    ...     proxy_url=transfer.proxy_url)

This will try to use `transfer_url`, and if it is not accessible, use
`proxy_url`.

### Dealing with certificates

By default, server certificate are verified using the provided `cafile`
argument. If you cannot get the right CA certificate file, or you trust
the environment and do not care about server verification, you can
disable server certificates verification by specifying secure=False in
both `upload()` and `download()`. In this case `cafile` can be set to
None.

Example upload without verifying server certificates:

    >>> client.upload(
    ...     "fedora-32.qcow2",
    ...     transfer.transfer_url,
    ...     None,
    ...     secure=False)


### Examples


## The low level API


### Connecting to imageio server


### Getting disk extents


### Reading data


### Writing data


### Zeroing a byte range


### Flushing changes


### Examples
