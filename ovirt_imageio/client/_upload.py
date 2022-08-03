# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Upload commands.
"""

import os
from contextlib import closing
from collections import namedtuple

from . import _api
from . import _options
from . import _ovirt
from . import _ui


DiskInfo = namedtuple(
    "DiskInfo",
    "name,initial_size,provisioned_size,content_type,format,sparse,is_zero")

FORMAT_RAW = "raw"
FORMAT_QCOW2 = "qcow2"
_DISK_FORMATS = (FORMAT_RAW, FORMAT_QCOW2)


def register(parser):
    cmd = parser.add_sub_command(
        "upload-disk",
        help="Upload disk",
        func=upload_disk)

    cmd.add_argument(
        "-s", "--storage-domain",
        required=True,
        help="Name of the storage domain.")

    cmd.add_argument(
        "-f", "--format",
        choices=_DISK_FORMATS,
        default=FORMAT_QCOW2,
        help="Upload disk format (default qcow2 for data disks and raw "
             "for iso disks).")

    cmd.add_argument(
        "--preallocated",
        dest="sparse",
        action="store_false",
        help="Create preallocated disk. Required when using raw format on "
             "block based storage domain (iSCSI, FC). ISO images are "
             "always uploaded to preallocated disk.")

    cmd.add_argument(
        "--disk-id",
        type=_options.UUID,
        help="A UUID for the new disk. If not specified, oVirt will "
             "create a new UUID.")

    cmd.add_argument(
        "--name",
        help="Alias name for the new disk. If not specified, name will "
             "correspond with the image filename.")

    cmd.add_argument(
        "filename",
        type=_options.File,
        help="Path to image to upload. Supported formats: raw, qcow2, iso.")


def upload_disk(args):
    with _ui.ProgressBar(phase="inspecting image") as progress:
        disk_info = _prepare(args)
        con = _ovirt.connect(args)
        with closing(con):
            progress.phase = "creating disk"
            disk = _ovirt.add_disk(
                con=con,
                name=disk_info.name,
                initial_size=disk_info.initial_size,
                provisioned_size=disk_info.provisioned_size,
                sd_name=args.storage_domain,
                id=args.disk_id,
                sparse=disk_info.sparse,
                enable_backup=disk_info.format == _ovirt.COW,
                content_type=disk_info.content_type,
                format=disk_info.format)

            progress.phase = "creating transfer"
            host = _ovirt.find_host(con, args.storage_domain)
            transfer = _ovirt.create_transfer(con, disk, host=host)
            try:
                progress.phase = "uploading image"
                _api.upload(
                    args.filename,
                    transfer.transfer_url,
                    args.cafile,
                    buffer_size=args.buffer_size,
                    progress=progress,
                    proxy_url=transfer.proxy_url,
                    max_workers=args.max_workers,
                    disk_is_zero=disk_info.is_zero)
            except Exception:
                progress.phase = "cancelling transfer"
                _ovirt.cancel_transfer(con, transfer)
                raise

            progress.phase = "finalizing transfer"
            _ovirt.finalize_transfer(con, transfer, disk)
        progress.phase = "upload completed"


def _prepare(args):
    # Obtain the image info dictionary.
    img_info = _api.info(args.filename)
    if img_info["format"] not in _DISK_FORMATS:
        raise RuntimeError(f"Unsupported image format {img_info['format']}")

    # Obtain all remaining fields for DiskInfo.
    if _is_iso(img_info["filename"], img_info["format"]):
        content_type = _ovirt.ISO
        # ISO images require raw format to work with cdrom device
        disk_format = FORMAT_RAW
        sparse = False
    else:
        content_type = _ovirt.DATA
        disk_format = args.format
        sparse = args.sparse

    initial_size = None
    if disk_format == FORMAT_QCOW2 and sparse:
        initial_size = _api.measure(args.filename, disk_format)["required"]

    name = args.name
    if name is None:
        name = os.path.splitext(os.path.basename(img_info["filename"]))[0]

    # On file based storage new disk is always zero. On block based storage
    # only when using sparse qcow2 format. Since we don't know the type of the
    # storage domain, we cannot optimize all cases.
    is_zero = (disk_format == FORMAT_QCOW2) or sparse

    return DiskInfo(
        name=name,
        initial_size=initial_size,
        provisioned_size=img_info["virtual-size"],
        content_type=content_type,
        format=_ovirt.COW if disk_format == FORMAT_QCOW2 else _ovirt.RAW,
        sparse=sparse,
        is_zero=is_zero)


def _is_iso(filename, image_format):
    """
    Detect if disk content type is ISO

    ISO format structure
    ---------------------------------------------------------------------------
    offset    type    value       comment
    ---------------------------------------------------------------------------
    0x0000                        system area (e.g. DOS/MBR boot sector)
    0x8000    int8    0x01        primary volume descriptor type code
    0x8001    strA    "CD001"     primary volume descriptor indentifier
    0x8006    int8    0x01        primary volume desctptor version
    0x8007            0x00        unused field

    See https://wiki.osdev.org/ISO_9660#Overview_and_caveats for more info.

    Returns:
        bool: Source image is ISO
    """
    if image_format == FORMAT_RAW:
        with open(filename, "rb") as file:
            file.seek(0x8000)
            primary_volume_descriptor = file.read(8)
        if primary_volume_descriptor == b"\x01CD001\x01\x00":
            return True
    return False
