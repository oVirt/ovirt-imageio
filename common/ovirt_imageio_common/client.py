# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.


"""
client - helpers for uploading and downloading disks
"""

from __future__ import absolute_import

import errno
import io
import json
import os
import socket
import ssl

import six
from six.moves import http_client
from six.moves.urllib.parse import urlparse

from . compat import subprocess

# Higher values are more efficient, sending less requests, but may cause large
# delays in progress updates when storage does not support efficient zeroing.
# This will take about 6 second with LIO storage, and about 25 milliseconds
# for high end FC storage.
MAX_ZERO_SIZE = 1024**3


def upload(filename, url, cafile, buffer_size=128 * 1024, secure=True,
           progress=None):
    """
    Upload filename to url

    Args:
        filename (str): File name for upload
        url (str): Transfer url in this format:
            https://host:port/images/ticket-uuid
        cafile (str): Certificate file name, for example "ca.pem"
        buffer_size (int): Buffer size in bytes for reading from storage and
            sending data over HTTP connection. The efault value of 128 kB seems
            to give good performance in our tests, you may like to tweak it.
        secure (bool): True for verifying server certificate and hostname.
            Default is True.
        progress (function): Function accepting one integer argument for
            updating upload progress. The function will be called after every
            write or zero operation with the number bytes transferred.
    """
    transfer = _create_transfer(
        url, cafile, buffer_size=buffer_size, secure=secure, progress=progress)
    try:
        # If the server supports "zero", we can upload sparse files more
        # efficiently.
        with io.open(filename, "rb") as src:
            transfer["file"] = src
            if transfer["can_zero"]:
                _upload_sparse(transfer)
            else:
                _upload(transfer)
    finally:
        transfer["con"].close()


def _create_transfer(
        url, cafile, buffer_size=128 * 1024, secure=True, progress=None):
    url = urlparse(url)

    context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=cafile)

    if not secure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    transfer = {
        "buffer_size": buffer_size,
        "con": HTTPSConnection(url.netloc, context=context),
        "path": url.path,
        "progress": progress,
    }

    try:
        # Check the server capabilities for this image.
        server_options = _options(transfer)
        transfer["can_zero"] = "zero" in server_options["features"]
        transfer["can_flush"] = "flush" in server_options["features"]

        # Optimize using unix socket if possible.
        if ("unix_socket" in server_options and
                transfer["con"].is_local()):
            transfer["con"].close()
            transfer["con"] = UnixHTTPConnection(server_options["unix_socket"])
    except:  # NOQA: E722 (bare 'except')
        transfer["con"].close()
        raise

    return transfer


class HTTPSConnection(http_client.HTTPSConnection):
    """
    HTTPS connection using TCP_NO_DELAY on python 2.
    """

    if six.PY2:
        def connect(self):
            """
            Using TCP_NO_DELAY avoids delays when sending small payload, such
            as an ovirt PATCH requests.

            This issue was fixed in python 3.5, see:
            https://bugs.python.org/issue23302
            """
            http_client.HTTPSConnection.connect(self)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def is_local(self):
        """
        Return True if connected to the local host.
        """
        # Hack for daemon versions 1.4.0 and 1.4.1 that supported unix
        # socket but not keep alive connections. With these versions the
        # socket is closed after calling getresponse().
        if self.sock is None:
            self.connect()

        return self.sock.getsockname()[0] == self.sock.getpeername()[0]


class UnixHTTPConnection(http_client.HTTPConnection):
    """
    HTTP connection over unix domain socket.
    """

    def __init__(self, path, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.path = path
        extra = {}
        if six.PY2:
            extra['strict'] = True
        http_client.HTTPConnection.__init__(
            self, "localhost", timeout=timeout, **extra)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
            self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def _upload_sparse(transfer):
    """
    Upload a possibly sparse file by sending the data portions using PUT
    request, and reconstructing the holes on the server side using
    PATCH/zero, without reading the zeros from the disk, or sending them
    over the wire.

    This works with ovirt-imageio 1.3 or later.
    """
    out = subprocess.check_output([
        "qemu-img",
        "map",
        "--format", "raw",
        "--output", "json",
        transfer["file"].name
    ])
    chunks = json.loads(out.decode("utf-8"))

    # If the server supports "flush", these requests are not waiting
    # until the data is flushed the underlying storage.
    for chunk in chunks:
        if chunk["data"]:
            _put(transfer, chunk["start"], chunk["length"])
        else:
            _zero(transfer, chunk["start"], chunk["length"])

    #  If flush option is supported flush once after sending all the data.
    if transfer["can_flush"]:
        _flush(transfer)


def _upload(transfer):
    """
    Upload a file using dumb PUT request. Holes in the file are read
    from disk and sent over the wire, and converted to allocated sectors
    full with zeros on the server.

    This works with older versions of ovirt-imageio proxy and daemon.
    """
    _put(transfer, 0, os.path.getsize(transfer["file"].name))
    # If flush option is supported flush once after sending all the data.
    if transfer["can_flush"]:
        _flush(transfer)


def _put(transfer, start, length):
    """
    Send a byte range from path to the server using a PUT request.

    If the server supports the "flush" feature, disable flushing for
    this request.
    """
    path = transfer["path"]
    if transfer["can_flush"]:
        path += "?flush=n"

    transfer["con"].putrequest("PUT", path)
    transfer["con"].putheader("content-type", "application/octet-stream")
    transfer["con"].putheader("content-length", "%d" % length)
    transfer["con"].putheader(
        "content-range", "bytes %d-%d/*" % (start, start + length - 1))
    transfer["con"].endheaders()

    transfer["file"].seek(start)

    pos = 0
    while pos < length:
        n = min(length - pos, transfer["buffer_size"])
        chunk = transfer["file"].read(n)
        if not chunk:
            raise RuntimeError(
                "Unexpected end of file, sent %d of %d bytes"
                % (pos, length))
        try:
            transfer["con"].send(chunk)
        except socket.error as e:
            if e[0] != errno.EPIPE:
                raise
            # Server closed the socket.
            break
        pos += len(chunk)

        if transfer["progress"]:
            transfer["progress"](len(chunk))

    res = transfer["con"].getresponse()
    error = res.read()
    if res.status != http_client.OK:
        raise RuntimeError("put chunk failed: %s" % error)


def _zero(transfer, start, length):
    """
    Zero a byte range on the server using a PATCH request.

    If the server supports "flush" feature, disable flushing for this
    request.
    """
    while length:
        step = min(MAX_ZERO_SIZE, length)
        msg = {"op": "zero",
               "offset": start,
               "size": step,
               "flush": not transfer["can_flush"]}
        _patch(transfer, msg)

        start += step
        length -= step

        if transfer["progress"]:
            transfer["progress"](step)


def _flush(transfer):
    """
    Flush data to underlying storage using a PATCH request.
    """
    msg = {"op": "flush"}
    _patch(transfer, msg)


def _patch(transfer, msg):
    """
    Send a PATCH request with specified message.
    """
    body = json.dumps(msg).encode("utf-8")
    headers = {"content-type": "application/json",
               "content-length": "%d" % len(body)}
    transfer["con"].request(
        "PATCH", transfer["path"], body=body, headers=headers)
    res = transfer["con"].getresponse()
    error = res.read()

    if res.status != http_client.OK:
        raise RuntimeError("patch %s failed: %s" % (msg, error))


def _options(transfer):
    """
    Send an OPTIONS request and return the features supported by the
    server for the specified path.
    """
    transfer["con"].request("OPTIONS", transfer["path"])
    res = transfer["con"].getresponse()
    body = res.read()

    default = {"features": []}

    if res.status == http_client.METHOD_NOT_ALLOWED:
        # Older daemon did not implement OPTIONS
        return default
    elif res.status == http_client.NO_CONTENT:
        # Older proxy did implement OPTIONS but does not return any content.
        return default
    elif res.status != http_client.OK:
        raise RuntimeError(
            "options %s failed: %s" % (transfer["path"], body))

    # New daemon or proxy provide a features list.
    try:
        msg = json.loads(body.decode("utf-8"))
    except ValueError:
        # Bad response, we must assume we don't support any features or unix
        # socket.
        return default

    return msg
