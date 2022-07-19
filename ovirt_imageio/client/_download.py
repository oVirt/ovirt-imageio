# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Download commands.
"""

from contextlib import closing

from .. _internal.units import KiB, MiB

from . import _api
from . import _options
from . import _ovirt
from . import _ui


def register(parser):
    cmd = parser.add_sub_command(
        "download-disk",
        help="Download disk",
        func=download_disk)

    cmd.add_argument(
        "-f", "--format",
        choices=("raw", "qcow2"),
        default="qcow2",
        help="Download image format (default qcow2).")

    size = _options.Size(minimum=1, default=_api.MAX_WORKERS, maximum=8)
    cmd.add_argument(
        "--max-workers",
        type=size,
        default=size.default,
        help=f"Maximum number of workers (range: {size.minimum}-"
             f"{size.maximum}, default: {size.default}).")

    size = _options.Size(
        minimum=64 * KiB, default=_api.BUFFER_SIZE, maximum=16 * MiB)
    cmd.add_argument(
        "--buffer-size",
        type=size,
        default=size.default,
        help=f"Buffer size per worker (range: {size.minimum}-"
             f"{size.maximum}, default: {size.default}).")

    cmd.add_argument(
        "disk_id",
        help="Disk ID to download.")

    cmd.add_argument(
        "filename",
        help="Target filename.")


def download_disk(args):
    con = _ovirt.connect(args)
    with closing(con):
        disk = _ovirt.find_disk(con, args.disk_id)
        storage_domain = _ovirt.find_storage_domain(con, disk)
        host = _ovirt.find_host(con, storage_domain.name)

        transfer = _ovirt.create_transfer(
            con, disk, direction=_ovirt.DOWNLOAD, host=host)
        try:
            with _ui.ProgressBar() as pb:
                _api.download(
                    transfer.transfer_url,
                    args.filename,
                    args.cafile,
                    fmt=args.format,
                    proxy_url=transfer.proxy_url,
                    max_workers=args.max_workers,
                    buffer_size=args.buffer_size,
                    progress=pb)
        finally:
            _ovirt.finalize_transfer(con, transfer, disk)
