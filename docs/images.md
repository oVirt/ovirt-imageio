---
redirect_from:
  - /random-io
---
# Images API

## Overview

The /images/ resource allows transferring virtual disk images and
VM backups when using oVirt backup API.

When starting an image transfer in oVirt, you get a transfer URL like:

    https://server:54322/images/74ca180f-77e5-44be-9196-e3226104e406

The last part of the URL is the ticket-id, created for every transfer.
oVirt takes care of attaching a disk to the host serving the transfer
and installing a ticket allowing access to the transfer URL.

When you connect to an imageio server, the first thing you should do is
send an OPTIONS request to learn about the capabilities of the server.
Based on the server capabilities, you can upload or download data in an
efficient way using GET, PUT and PATCH requests.

Once the image transfer is finished, the transfer URL cannot be
accessed.

## OPTIONS

Describes the available methods for a resource.

The results depend on the resource and the server:

- The special `*` ticket_id: returns the options supported by the
  server, assuming a writable ticket allowing all features.

- A concrete ticket-id: returns the options available for this image.
  For example, if the image is read-only, the server will not report the
  `PUT` and `PATCH` methods, and the feature list will not include
  the `zero` and `flush` features.

The application should inspect the "Allow" header for allowed methods,
and the returned JSON document for available features and options.

### features

The features supported by the server are reported in the `features`
list:

- `zero`: PATCH/zero request is supported.
- `flush`: The application can control flushing in PUT and PATCH
  requests or send PATCH/flush request.
- `extents`: Getting image extents is supported.

### unix_socket

If the server listens also on a Unix socket, the Unix socket address is
returned in the `unix_socket` key in the response. If the client runs on
the same host, and the host can perform the image transfer, it should
transfer the image data using the Unix socket for improved throughput
and lower CPU usage.

### max_readers

If the server supports multiple connections and the ticket is specifying
a backend supporting multiple readers (nbd, file) it will report the
maximum number of connections in that can read a single image
concurrently.

If the server does not report the `max_readers` option it does not
support multiple connections and using multiple readers may fail.

### max_writers

If the server supports multiple connections and the ticket is specifying
a backend supporting multiple writers (nbd) it will report the maximum
number of connections in that can write to a single image concurrently.

If the server does not report the `max_writers` option it does not
support multiple connections and using multiple writers may fail and
corrupt image data.

When using multiple writers, each writer should modify a distinct byte
range. If two writers modify the same byte range concurrently they will
overwrite each other data.

### Errors

Specific errors for OPTIONS request:

- "405 Method Not Allowed": This is an old imageio server that did not
  implement OPTIONS. This server supports only GET and PUT.

- "204 No Content": This is an old imageio proxy server. This server
  supports only GET and PUT.

### Version info

Since 1.3

### Examples

Request:

    OPTIONS /images/{ticket-id} HTTP/1.1

Response:

    HTTP/1.1 200 OK
    Allow: GET,PUT,PATCH,OPTIONS
    Content-Type: application/json
    Content-Length: 122

    {"unix_socket": "\u0000/org/ovirt/imageio", "features": ["extents", "zero", "flush"],
     "max_readers": 8, "max_writers": 8}

Get options for ticket-id with read-write access using nbd backend:

    $ curl -k -X OPTIONS https://server:54322/images/{ticket-id} | jq
    {
      "unix_socket": "\u0000/org/ovirt/imageio",
      "features": [
        "extents",
        "zero",
        "flush"
      ],
      "max_readers": 8,
      "max_writers": 8
    }

The nbd backend is used when specifying the "raw" transfer format when
creating an image transfer in oVirt API.

Get options for ticket-id with read-only access using file backend:

    $ curl -sk -X OPTIONS https://server:54322/images/{ticket-id} | jq
    {
      "unix_socket": "\u0000/org/ovirt/imageio",
      "features": [
        "extents",
      ],
      "max_readers": 8,
      "max_writers": 1
    }

Get all available options for the special `*` ticket:

    $ curl -sk -X OPTIONS 'https://server:54322/images/*' | jq
    {
      "unix_socket": "\u0000/org/ovirt/imageio",
      "features": [
        "extents",
        "zero",
        "flush"
      ]
    }


## GET

Downloads byte range {start} to {end} from the image associated with
ticket-id. The length of the response body is ({end} - {start} + 1).

Multiple ranges are not supported. Requesting multiple ranges will fail
with "416 Range Not Satisfiable".

Downloading an entire image is not efficient; the recommended way to is
to get the image extents and download only the needed extents.

### Version info

Since 0.5

### Examples

Request:

    GET /images/{ticket-id}
    Range: bytes={start}-{end}

Response:

    HTTP/1.1 206 Partial Content
    Content-Type: application/octet-stream
    Content-Range: bytes {start}-{end}/*
    Content-Length: {length}

    <{length} bytes of image data>

Download entire image:

    $ curl -k https://server:54322/images/{ticket-id} > disk.img

Download 64 KiB extent starting at 2 MiB:

    $ curl -k --range 2097152-2162687 https://server:54322/images/{ticket-id} > extent


## EXTENTS

The extents API returns information about image content and allocation
or about changed blocks during an incremental backup.

To get image extents send a GET request to the /extents sub-resource of
the transfer URL:

    GET /images/{ticket-id}/extents

### Query string

- `context`: zero|dirty - Specify `zero` if you want to get zero
  extents or `dirty` if you want to get dirty extents. Dirty extents
  are available only during an incremental backup. If not specified,
  defaults to `zero`.

### Zero extent

Describes image content and allocation. If the `zero` flag is
`true`, the extent is a zero extent; otherwise this is a data extent.
A zero extent is an area on storage that reads as zeroes. The storage
area may be allocated or not.

If the extent content is zero, it may be a hole. A hole extent is
reported only for qcow2 images, when transferring the image without the
backing file. For raw images unallocated areas are never reported as
holes.

Properties:
- `start`: The offset in bytes from the start of the image.
- `length`: The length in bytes.
- `zero`: true if the extent reads as zeroes; false if the extent is
  data.
- `hole`: true if the extent is unallocated areas in a qcow2 image,
  exposing data from the backing chain.

### Dirty extent

Describes the change status of an extent during an incremental backup.
If the `dirty` flag is true, the extent was modified and should be
downloaded in this incremental backup. If the `dirty` flag is false, the
extent did not change and it should not be downloaded during this
backup.

If a dirty extent has the `"zero": true` flag, this extent is read as
zeroes, so there is no need to download it. You must write zeroes to
this byte range in the backup, but this can be done in an efficient way.

Properties:
- `start`: The offset in bytes from the start of the image.
- `length`: The length in bytes.
- `dirty`: true if the extent was modified and should be downloaded in
  this incremental backup.
- `zero`: true if the extent reads as zeroes; false if the extent is
  data (since 2.2.0-1).

### Errors

Specific errors for EXTENTS request:

- "404 Not Found": If context=dirty was specified when the image
  transfer is not part of an incremental backup.

### Version info

Since 2.0

### Examples

Request zero extents:

    GET /images/{ticket-id}/extent

Response:

    HTTP/1.1 200 OK
    Content-Length: 67
    Content-Type: application/json

    [{"start": 0, "length": 107374182400, "zero": true, "hole": false}]

Request dirty extents:

    GET /images/{ticket-id}/extent?context=dirty

Response:

    HTTP/1.1 200 OK
    Content-Length: 206
    Content-Type: application/json

    [{"start": 0, "length": 65536, "dirty": true, "zero": false},
    {"start": 65536, "length": 1073676288, "dirty": false, "zero": false},
    {"start": 1073741824, "length": 1073741824, "dirty": true, "zero": true}]

Getting extents for empty 100 GiB image:

    $ curl -sk https://server:54322/images/nbd/extents | jq
    [
      {
        "start": 0,
        "length": 107374182400,
        "zero": true,
        "hole": false
      }
    ]

Getting extents during incremental backup:

    $ curl -sk 'https://server:54322/images/nbd/extents?context=dirty' | jq
    [
      {
        "start": 0,
        "length": 65536,
        "dirty": true,
        "zero": false
      }
      {
        "start": 65536,
        "length": 1073676288,
        "dirty": false,
        "zero": false
      }
      {
        "start": 1073741824,
        "length": 1073741824,
        "dirty": false,
        "zero": true
      }
    ]

The first extent was modified and should be downloaded in this
incremental backup. The second extent was not modified and should not be
downloaded in this incremental backup. The third extent was modified but
it is read as zeroes, so you don't need to download it, but you must
write zeroes to this area in the backup.


## PUT

Uploads {length} bytes at offset {start} in the image associated with
{ticket-id}.

By default, data is flushed to the underlying storage before returning a
response. If you want to defer flushing you may specify the flush=n
query string. In this case, you need to either flush in the last PUT or
use PATCH to flush once at the end of the transfer.

### Query string

- `flush`: "y|n" - Flush data before responding, assumes "y" if not
  specified. Effective only if the server supports the `flush` feature.
  Older versions of the daemon and proxy ignore this parameter and will
  flush after every request (new in 1.3).

### Version info

Since 0.2

### Examples

Request:

    PUT /images/{ticket-id}?flush=y|n
    Content-Range: bytes {start}-{end}/*
    Content-Length: {length}

    <{length} bytes of image data>

Response:

    HTTP/1.1 200 OK

Upload an entire image and flush data to storage:

    $ curl -k -X PUT --upload-file disk.img https://server:54322/images/{ticket-id}

curl does support uploading part of a file, but if you downloaded
an extent using GET request you can upload it back by specifying the
Content-Range header.

Upload 64 KiB extent starting at 2 MiB, without flushing to storage:

    $ curl -k -X PUT \
        --upload-file extent \
        --header "Content-Range: bytes 2097152-2162687/*" \
        https://server:54322/images/{ticket-id}?flush=n

Upload 1 GiB extent starting at offset 5 GiB, and flush data to storage:

    $ curl -k -X PUT \
        --upload-file extent \
        --header "Content-Range: bytes 5368709120-6442450943/*" \
        https://server:54322/images/{ticket-id}?flush=y

Note that the server uses only the start byte from the Content-Range
header. The length of the upload is taken from the "Content-Length"
header which is required.


## PATCH

Patch a byte range in the image associated with {ticket-id}.

The request accepts application/json message describing the byte range
to patch, and how to patch it.

### Version info

Since 1.3

### Zero operation

Zero a byte range without sending the actual zeros over the wire.

Zeroing ensures that the specified byte range will be read as zeroes
after the operation. Depending on the ticket "sparse" option and the
underlying storage capabilities, the operation may deallocate space
(punch hole), or allocate space (zero range).

If the underlying storage supports efficient zeroing the operation can
be done without doing any actual I/O very quickly. Otherwise, imageio
server will fall back to writing zeros manually which may be slow.

Properties:

- `op`: `zero`
- `size`: The size in bytes to operate on.
- `offset`: If specified, the offset in bytes from start of the image,
  otherwise 0.
- `flush`: if specified and true, flush data to storage before responding,
  otherwise data is not flushed. Effective only if the server supports
  the `flush` feature.

### Version info

Since 1.3

### Examples

Request:

    PATCH /images/{ticket-id}
    Content-Type: application/json
    Content-Length: 44

    {"op": "zero", "offset": 4096, "size": 8192}

Response:

    HTTP/1.1 200 OK

Zero 4096 bytes at offset 1 GiB without flushing data to storage:

    curl -k -X PATCH \
        --data-binary '{"op": "zero", "offset": 1073741824, "size": 4096}' \
        https://server:54322/images/{ticket-id}

Zero entire 100 GiB disk and flush changes to storage:

    curl -k -X PATCH \
        --data-binary '{"op": "zero", "size": 107374182400, "flush": true}' \
        https://server:54322/images/{ticket-id}

### Flush operation

Flush the data written to the image to the underlying storage. The call
returns only when the device reports that the transfer was done.
Operates on the entire image, "offset" and "size" are ignored.

Properties

- `op`: `flush`

### Version info

Since 1.3

### Examples

Request:

    PATCH /images/{ticket-id}
    Content-Type: application/json
    Content-Length: 15

    {"op": "flush"}

Response:

    HTTP/1.1 200 OK

Flush changes to storage:

    curl -k -X PATCH \
        --data-binary '{"op": "flush"}' \
        https://server:54322/images/{ticket-id}

## General errors

General errors that may be returned from all APIs:

- "416 Range Not Satisfiable": Range or Content-Range headers exceeds
  the image size. The available content range is specified in the
  `Content-Range` header. You can retry the request with corrected
  range.

- "403 Forbidden": The ticket does not exist, has expired, or was
  canceled. You should not retry this request.

- "400 Bad Request": The request is invalid. You should not retry this
  request.

- "500 Internal Server Error": The server could not complete the
  request. When using PUT you should send again all the data sent since
  the last flush.

The response body adds more information on the issue and how it can be
resolved.

For internal server error, you need to check imageio server logs at
`/var/log/ovirt-imageio/daemon.log` to get more info about the error.

### Examples

Trying to access non-existing ticket:

    $ curl -k https://server:54322/images/no-such-ticket
    You are not allowed to access this resource: No such ticket no-such-ticket

Invalid request:

    $ curl -k -X PATCH \
        --data-binary '{"op": "zero", "offset": 4096}' \
        https://server:54322/images/nbd
    Missing required value for 'size'
