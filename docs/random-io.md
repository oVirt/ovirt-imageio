# Random I/O

## Use cases

Random I/O is needed for several use cases like resuming interrupted
upload or download, efficient upload of a sparse image, and integration
with virt-v2v.

### Resuming incomplete download

A client such as a browser can pause and resume a download. When
resuming, the browser sends a Range request[1] to get the rest of the
image.

[1] https://tools.ietf.org/html/rfc7233

### Resuming incomplete upload

An application that wishes to resume an upload after interruptions can
upload an image in multiple PUT requests with a Content-Range header,
specifying the start offset for every request.

When the application receives a response for a PUT request, the data
uploaded was already flushed to storage. If the upload is interrupted,
the application can resume it after the last successful PUT request.

### Efficient upload of a sparse image

To upload a sparse image, the application can use tools such as
"qemu-img map" to find the data and holes in an image. For each data
range, the application can upload the data using a PUT request. For each
hole, the application can send a PATCH/zero request to zero the byte
range on the server, without sending the actual zeros.

In this case the application may need to preform lot of small requests,
so flushing the data to the underlying storage on each request may be
wasteful. The application can disable flushing in PUT and PATCH request
to increase the upload throughput. However if the application disabled
flushing, it is responsible for doing at least one flush at the end of
the upload, using either a PUT or PATCH request with flush enabled, or a
PATCH/flush request.

If the application wishes to resume the upload after interruptions, it
can send multiple explicit PATCH/flush requests. If the upload is
interrupted the application can resume the upload after the last
successful PATCH/flush request.

### Integration with virt-v2v

virt-v2v is using "qemu-img convert" and "nbdkit" to stream an image in
any format supported by qemu-img to oVirt image. The flow looks like:

    qemu-img convert --> nbdkit --> ovirt-imageio

nbdkit maps "pread", "pwrite", "zero", "trim", and "flush" NBD calls to
GET, PUT, PATCH/zero, PATCH/trim[1], and PATCH/flush requests for
ovirt-imageio. In this case qemu-img controls the flow of calls, and
ovirt-imageio should be able to perform the calls in an efficient way,
avoiding unnecessary flushes or sending zeros over the wire.

[1] PATCH/trim requests are not supported yet by ovirt-imageio.

### Detecting capabilities

To detect if a remote imageio daemon or proxy support the random I/O
APIs, the application should issue an OPTIONS request and inspect the
response for the allowed methods and available features.


## OPTIONS

Describes the available methods for a resource. This is implemented
currently only for the ``/images`` resource.

The results depend on the resource and the server:

- The special ``*`` ticket_id: returns the options supported by the
  server, assuming a writable ticket allowing all features.

- A concrete ticket-id: returns the options available for this image.
  For example, if the image is readonly, the server will not report the
  PUT and PATCH methods, and the feature list will be empty.

- When querying the proxy about the special ``*`` ticket-id, the
  response is the options supported by the proxy, since the URL of the
  daemon is part of the ticket. When querying the proxy about a concrete
  ticket-id, the proxy will send the request to the daemon and return
  the response returned by the daemon.

- If the proxy and daemon capabilities differ (e.g. new proxy and old
  daemon), the results will include only capabilities supported by both
  the proxy and the daemon.

The application should inspect the "Allow" header for allowed methods,
and the returned JSON document for available features.

Available features:

- zero: PATCH/zero request is supported
- flush: the application can control flushing in PUT and PATCH requests
  or send PATCH/flush request.

Older versions of the daemon did not implement this method, so the
request would fail with "405 Method Not Allowed".

Older versions of the proxy did implement OPTIONS but returned "204 No
Content".

If the ticket does not exist the request will fail with "403 Forbidden".

Request:

    OPTIONS /images/* HTTP/1.1

Response:

    HTTP/1.1 200 OK
    Allow: GET,PUT,PATCH,OPTIONS
    Content-Type: application/json
    Content-Length: LENGTH

    {
        "features": ["zero", "flush"]
    }

Since: 1.3


## GET

Downloads bytes START-END from the image associated with TICKET-ID. The
length of the response body is END - START + 1.

We do not support multiple ranges because we don't have a good use case
for them yet. Requesting multiple ranges will fail with "416 Range Not
Satisfiable".

Request:

    GET /images/TICKET-ID
    Range: bytes=START-END

Response:

    HTTP/1.1 206 Partial Content
    Content-Type: application/octet-stream
    Content-Range: bytes START-END/*
    Content-Length: LENGTH

    <LENGTH bytes of image data>

Since: 0.5


## PUT

Uploads LENGTH message body bytes at offset START in the image
associated with TICKET-ID.

By default data is flushed to the underlying storage before returning a
response. If you want to defer flushing you may specify the flush=n
query string. In this case you need to either flush in the last PUT or
use PATCH to flush once at the end of the transfer.

Query string:
- flush: "y|n" - flush data before responding, assumes "y" if not
  specified. Effective only if the server supports the "flush" feature.
  Older versions of the daemon and proxy ignore this parameter and will
  flush after every request (new in 1.3).

Request:

    PUT /images/TICKET-ID?flush=y|n
    Content-Range: bytes START-END/*
    Content-Length: LENGTH

    <LENGTH bytes of image data>

Response:

    HTTP/1.1 200 OK

Since: 0.2


## PATCH

Patch a byte ranges in the image associated with TICKET-ID.

The request accepts application/json message describing the byte range
to patch, and how to patch it.

Since: 1.3


### Zero operation

Zero a byte range without sending the actual zeros over the wire.

Properties:
- op: "zero"
- size: size of the byte range to operate on
- offset: if specified, start of the byte range, otherwise 0
- flush: if specified and true, flush data to storage before responding,
  otherwise data is not flushed. Effective only if the server supports
  the "flush" feature.

Request:

    PATCH /images/TICKET-ID
    Content-Type: application/json
    Content-Length: LENGTH

    {
        "op": "zero",
        "offset": 4096,
        "size": 8192,
        "flush": false
    }

Response:

    HTTP/1.1 200 OK

Since: 1.3


### Flush operation

Flush the data written to the image to the underlying storage. The call
returns only when the device reports that the transfer was done.
Operates on the entire image, "offset" and "size" are ignored.

Properties:

- op: "flush"

Request:

    PATCH /images/TICKET-ID
    Content-Type: application/json
    Content-Length: LENGTH

    {
        "op": "flush"
    }

Response:

    HTTP/1.1 200 OK

Since: 1.3
