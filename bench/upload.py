#!/usr/bin/python3

import json
import os
import subprocess
import sys
import time

from contextlib import contextmanager

# TODO: Find a better way to use package from source, so we can remove the
# "noqa" from the imports bellow.
sys.path.insert(0, ".")

from ovirt_imageio import admin  # noqa: E402
from ovirt_imageio._internal import nbd  # noqa: E402
from ovirt_imageio._internal import qemu_img  # noqa: E402
from ovirt_imageio._internal import qemu_nbd  # noqa: E402

CONF = "bench"
NBD_SOCK = os.path.abspath("bench/ndb.sock")
TICKET = "bench/upload.json"

# Use different drives for source and destiantion image for best results.
SRC_IMG = "/data/scratch/fedora-35-data.raw"
DST_IMG = "/data/tmp/dst.raw"

IMG_SIZE = 6*1024**3
TRANSFER_URL = "https://localhost:54322/images/upload"

# Consume NVMe drives quickly slow down after writing few GiBs. Waiting for the
# drive to cool down before running the test keeps results stable. If you have
# enterprize grade drive, you can set this to 0.
COOLDOWN_DELAY = 30

# Use higher value for more reliable results.
RUNS = 3

profile = False


@contextmanager
def imageio_server(conf):
    server = subprocess.Popen(["./ovirt-imageio", "--conf-dir", conf])
    try:
        # TODO: wait for server socket.
        time.sleep(1)
        yield
    finally:
        server.terminate()
        server.wait()


cfg = admin.load_config("bench")

qemu_img.create(DST_IMG, "raw", size=IMG_SIZE)

with qemu_nbd.run(DST_IMG, "raw", nbd.UnixAddress(NBD_SOCK)), \
        imageio_server(CONF), \
        admin.Client(cfg) as adm:

    with open(TICKET) as f:
        data = f.read()
    ticket = json.loads(data)
    ticket["url"] = f"nbd:unix:{NBD_SOCK}"
    ticket["size"] = IMG_SIZE

    adm.add_ticket(ticket)

    if profile:
        adm.start_profile()

    # NOTE: Without --show-output hyperfine seems to show ~0.2s extra
    # time which is a lot when uploading small images, and displayed
    # time is much less stable (e.g. +-0.257 vs +-0.028).

    cmd = [
        "hyperfine",
        "--runs", f"{RUNS}",
        "--prepare", f"sleep {COOLDOWN_DELAY}",
        "--parameter-list", "b", "256,512,1024,2048,4096,8192",
        "--show-output",
        "examples/imageio-client --insecure --quiet --buffer-size={b} "
        f"upload {SRC_IMG} {TRANSFER_URL}"
    ]
    subprocess.run(cmd, check=True)

    if profile:
        adm.stop_profile()
