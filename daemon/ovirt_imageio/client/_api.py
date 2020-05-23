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

import logging
import os
import shutil
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
           progress=None, proxy_url=None):
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
    """
    if callable(progress):
        progress = ProgressWrapper(progress)

    info = qemu_img.info(filename)
    if progress:
        progress.size = info["virtual-size"]

    with _open_nbd(filename, info["format"], read_only=True) as src, \
            _open_http(
                url,
                "w",
                cafile=cafile,
                secure=secure,
                proxy_url=proxy_url) as dst:
        io.copy(src, dst, buffer_size=buffer_size, progress=progress)


def download(url, filename, cafile, fmt="qcow2", incremental=False,
             buffer_size=io.BUFFER_SIZE, secure=True, progress=None,
             proxy_url=None):
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
    """
    if incremental and fmt != "qcow2":
        raise ValueError(
            "incremental={} is incompatible with fmt={}"
            .format(incremental, fmt))

    with _open_http(
            url,
            "r",
            cafile=cafile,
            secure=secure,
            proxy_url=proxy_url) as src:
        size = src.size()
        if progress:
            progress.size = size

        qemu_img.create(filename, fmt, size=size)

        with _open_nbd(filename, fmt) as dst:
            # We created new empty file, no need to zero.
            io.copy(
                src,
                dst,
                dirty=incremental,
                buffer_size=buffer_size,
                zero=False,
                progress=progress)


class ProgressWrapper:
    """
    In older versions we supported passing an update() callable instead of an
    object with update() method. Wrap the callable to make it work with current
    code.
    """
    def __init__(self, update):
        self.update = update


@contextmanager
def _open_nbd(filename, fmt, read_only=False):
    with _tmp_dir("imageio-") as base:
        sock = UnixAddress(os.path.join(base, "sock"))
        with qemu_nbd.run(
                filename,
                fmt,
                sock,
                read_only=read_only,
                cache=None,
                aio=None,
                discard=None):
            nbd_url = urlparse(sock.url())
            mode = "r" if read_only else "r+"
            yield nbd.open(nbd_url, mode)


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
