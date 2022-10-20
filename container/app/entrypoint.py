#!/venv/bin/python3
#
# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import argparse
import json
import os
import subprocess

from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import nbd
from ovirt_imageio._internal import util

TICKET_PATH = "/app/ticket.json"
OVIRT_IMAGEIO = "/venv/bin/ovirt-imageio"


def main():
    args = _parse_args()
    image_info = qemu_img.info(args.image)
    with util.tmp_dir("imageio-") as base:
        socket_path = os.path.join(base, "sock")

        ticket_info = _generate_ticket(
            args.ticket_id, socket_path, image_info["virtual-size"])
        with open(TICKET_PATH, "w") as f:
            f.write(json.dumps(ticket_info))

        sock = nbd.UnixAddress(socket_path)
        with qemu_nbd.run(args.image, image_info["format"], sock):
            subprocess.run(
                [OVIRT_IMAGEIO, "--ticket", TICKET_PATH], check=True)


def _generate_ticket(uuid, socket, size):
    ticket_info = {}
    ticket_info["uuid"] = uuid
    ticket_info["url"] = f"nbd:unix:{socket}"
    ticket_info["size"] = size
    ticket_info["timeout"] = 3000
    ticket_info["inactivity_timeout"] = 300
    ticket_info["ops"] = ["read"]
    return ticket_info


def _parse_args():
    parser = argparse.ArgumentParser(description="ovirt-imageio")
    parser.add_argument("image", help="Path to image.")
    parser.add_argument(
        "--ticket-id",
        help="Optional. Set the ID of the ticket. It is recommended "
        "to use with random string for better security. "
        "Defaults to the image filename.")

    args = parser.parse_args()
    # Ticket ID defaults to image filename if not set.
    args.ticket_id = args.ticket_id or os.path.basename(args.image)
    return args


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted, shutting down")
