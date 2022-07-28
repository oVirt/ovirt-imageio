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

    cmd.add_argument(
        "disk_id",
        type=_options.UUID,
        help="Disk ID to download.")

    cmd.add_argument(
        "filename",
        help="Target filename.")


def download_disk(args):
    with _ui.ProgressBar(phase="creating transfer") as pb:
        con = _ovirt.connect(args)
        with closing(con):
            disk = _ovirt.find_disk(con, args.disk_id)
            storage_domain = _ovirt.find_storage_domain(con, disk)
            host = _ovirt.find_host(con, storage_domain.name)

            transfer = _ovirt.create_transfer(
                con, disk, direction=_ovirt.DOWNLOAD, host=host)
            try:
                pb.phase = "downloading image"
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
                pb.phase = "finalizing transfer"
                _ovirt.finalize_transfer(con, transfer, disk)
        pb.phase = "download completed"
