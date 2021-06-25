# Packages

## ovirt-imageio-daemon

This package provides the ovirt-imageio service, exposing oVirt disks
via HTTPS protocol. This service is used for downloading disks,
uploading disk images, and incremental backup.

### Services

The server provides these services:

- `remote`: Access images via HTTPS protocol. This is the only way to
  access images remotely.

- `local`: Access images via HTTP over Unix socket. This is the most
  performant way if the client runs on the same host as the server.

- `control`: Access transfer tickets via HTTP over Unix socket or TCP.

### Endpoints

The server exposes these endpoints:

#### /images

Allows client to read disk data or write data to disks. This endpoint is
available remotely or locally. Accessing this endpoint locally provides
better performance and minimizes network bandwidth when transferring
images.

See [Images API](images.md) for more info.

#### /info

Provides information about the server. Used by ovirt-engine to check
connectivity to the host.

#### /tickets

Manages transfer tickets providing access to the `/images` endpoint.
Available only locally either via HTTP over Unix domain socket, or TCP
port 5324.

On oVirt host, vdsm uses this endpoint via Unix socket to add, remove,
and extend tickets per ovirt-engine request. On oVirt engine host,
engine uses this endpoint via TCP port do add, remove, and extent
tickets.

This endpoint is not a public interface and should be used only by the
program managing ovirt-imageio-daemon (e.g. vdsm, ovirt-engine).

### Backends

The `/images` endpoint supports several backends:

- `nbd`: Exposes disks exported via NBD protocol. This backend provides
  best performance and advanced features like zero and dirty extents,
  and computing image checksums.

- `file`: Exposes disks by accessing the disks directly. This
  backend does not support multiple connections when writing data and
  cannot report zero or dirty extents.

- `http`: Exposes remote images on another oVirt host. Used when running
  as a proxy on engine host. This backend provides all features supported
  by the origin server with reduced performance.

The backend for a transfer is controlled by transfer ticket "url"
property.

## ovirt-imageio-client

This package provides the python `ovirt_imageio` library. This library
can be used to access ovirt-imageio images API on local or remote host
without writing low level HTTP code.

## ovirt-imageio-common

This package provides common code use by `ovirt-imageio-daemon` and
`ovirt-imageio-client` packages.
