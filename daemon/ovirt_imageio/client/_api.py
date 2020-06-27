# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
api - imageio public client API.
"""

from __future__ import absolute_import

import json
import logging
import os
import shutil
import tarfile
import tempfile

from contextlib import contextmanager
from urllib.parse import urlparse

from .. _internal import io
from .. _internal import qemu_img
from .. _internal import qemu_nbd
from .. _internal.backends import http, nbd
from .. _internal.nbd import UnixAddress

log = logging.getLogger("client")


def upload(filename, url, cafile, buffer_size=io.BUFFER_SIZE, secure=True,
           progress=None, proxy_url=None, max_workers=io.MAX_WORKERS):
    """
    Upload filename to url

    Args:
        filename (str): File name for upload
        url (str): Transfer url on the host running imageio server
            e.g. https://{imageio.server}:{port}/images/{ticket-id}.
        cafile (str): Certificate file name, for example "ca.pem"
        buffer_size (int): Buffer size in bytes for reading from storage and
            sending data over HTTP connection.
        secure (bool): True for verifying server certificate and hostname.
            Default is True.
        progress (client.ProgressBar): an object implementing
            client.ProgressBar() interface.  progress.size attribute will be
            set when upload size is known, and then progress.update() will be
            called after every write or zero operation with the number bytes
            transferred.  For backward compatibility, we still support passing
            an update callable.
        proxy_url (str): Proxy url on the host running imageio as proxy, used
            if url is not accessible.
            e.g. https://{proxy.server}:{port}/images/{ticket-id}.
        max_workers (int): Maximum number of worker threads to use.
    """
    if callable(progress):
        progress = ProgressWrapper(progress)

    info = qemu_img.info(filename)

    # Open the destination backend to get number of workers.
    with _open_http(
            url,
            "r+",
            cafile=cafile,
            secure=secure,
            proxy_url=proxy_url) as dst:

        max_workers = min(dst.max_writers, max_workers)

        # Open the source backend using avialable workers + extra worker used
        # for getting image extents.
        with _open_nbd(
                filename,
                info["format"],
                read_only=True,
                shared=max_workers + 1) as src:

            # Upload the image to the server.
            io.copy(
                src,
                dst,
                max_workers=max_workers,
                buffer_size=buffer_size,
                progress=progress,
                name="upload")


def download(url, filename, cafile, fmt="qcow2", incremental=False,
             buffer_size=io.BUFFER_SIZE, secure=True, progress=None,
             proxy_url=None, max_workers=io.MAX_WORKERS):
    """
    Download url to filename.

    Args:
        url (str): Transfer url on the host running imageio server
            e.g. https://{imageio.server}:{port}/images/{ticket-id}.
        filename (str): Where to store downloaded data.
        cafile (str): Certificate file name, for example "ca.pem"
        fmt (str): Download file format ("raw", "qcow2"). The default is
            "qcow2" is usually the best option, supporting sparsness regardless
            of the local file system, and incremental backups.
        incremental (bool): Download only changed blocks. Valid only during
            incremetnal backup and require format="qcow2".
        buffer_size (int): Buffer size in bytes for reading from storage and
            sending data over HTTP connection.
        secure (bool): True for verifying server certificate and hostname.
            Default is True.
        progress (client.ProgressBar): an object implementing
            client.ProgressBar() interface.  progress.size attribute will be
            set when download size is known, and then progress.update() will be
            called after every read with the number bytes transferred.
        proxy_url (str): Proxy url on the host running imageio as proxy, used
            as if url is not accessible.
            e.g. https://{proxy.server}:{port}/images/{ticket-id}.
        max_workers (int): Maximum number of worker threads to use.
    """
    if incremental and fmt != "qcow2":
        raise ValueError(
            "incremental={} is incompatible with fmt={}"
            .format(incremental, fmt))

    # Open the source backend to get number of workers and image size.
    with _open_http(
            url,
            "r",
            cafile=cafile,
            secure=secure,
            proxy_url=proxy_url) as src:

        # Create a local image. We know that this image is zeroed, so we don't
        # need to zero during copy.
        qemu_img.create(filename, fmt, size=src.size())

        max_workers = min(src.max_readers, max_workers)

        # Open the destination backend.
        with _open_nbd(filename, fmt, shared=max_workers) as dst:

            # Download the image from the server to the local image.
            io.copy(
                src,
                dst,
                dirty=incremental,
                max_workers=max_workers,
                buffer_size=buffer_size,
                zero=False,
                progress=progress,
                name="download")


def info(filename, member=None):
    """
    Return image information.

    If member is specified, filename must be a tar file, and the call returns
    information about file named member inside the tar file, and the offset and
    size of the member in the tar file.
    """
    if member:
        offset, size = _find_member(filename, member)
        uri = _json_uri(filename, offset, size)
        info = qemu_img.info(uri)
        info["member-offset"] = offset
        info["member-size"] = size
        return info
    else:
        return qemu_img.info(filename)


def measure(filename, dst_fmt, member=None):
    """
    Measure required size for converting filename to dst_fmt.

    If member is specified, filename must be a tar file, and the call returns
    measurement about file named member inside the tar file.
    """
    if member:
        offset, size = _find_member(filename, member)
        uri = _json_uri(filename, offset, size)
        measure = qemu_img.measure(uri, dst_fmt)
        measure["member-offset"] = offset
        measure["member-size"] = size
        return measure
    else:
        return qemu_img.measure(filename, dst_fmt)


class ProgressWrapper:
    """
    In older versions we supported passing an update() callable instead of an
    object with update() method. Wrap the callable to make it work with current
    code.
    """
    def __init__(self, update):
        self.update = update


def _find_member(tarname, name):
    with tarfile.open(tarname) as tar:
        member = tar.getmember(name)
        return member.offset_data, member.size


def _json_uri(filename, offset, size):
    # Leave the top driver to enable format probing.
    # https://lists.nongnu.org/archive/html/qemu-discuss/2020-06/msg00094.html
    nodes = {
        "file": {
            "driver": "raw",
            "offset": offset,
            "size": size,
            "file": {
                "driver": "file",
                "filename": filename,
            }
        }
    }
    return "json:" + json.dumps(nodes)


@contextmanager
def _open_nbd(filename, fmt, read_only=False, shared=1):
    with _tmp_dir("imageio-") as base:
        sock = UnixAddress(os.path.join(base, "sock"))
        with qemu_nbd.run(
                filename,
                fmt,
                sock,
                read_only=read_only,
                cache=None,
                aio=None,
                discard=None,
                shared=shared):
            url = urlparse(sock.url())
            mode = "r" if read_only else "r+"
            yield nbd.open(url, mode=mode)


def _open_http(transfer_url, mode, cafile=None, secure=True, proxy_url=None):
    log.debug("Trying %s", transfer_url)
    url = urlparse(transfer_url)
    try:
        return http.open(url, mode, cafile=cafile, secure=secure)
    except OSError as e:
        if proxy_url is None:
            raise

        log.debug("Cannot open %s (%s), trying %s",
                  transfer_url, e, proxy_url)
        url = urlparse(proxy_url)
        return http.open(url, mode, cafile=cafile, secure=secure)


@contextmanager
def _tmp_dir(prefix):
    path = tempfile.mkdtemp(prefix=prefix)
    try:
        yield path
    finally:
        shutil.rmtree(path)
