# ovirt-imageio
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import json
import logging

from ovirt_imageio_common.compat import subprocess

log = logging.getLogger("qemu_img")


def create(path, fmt, size=None, backing=None):
    cmd = ["qemu-img", "create", "-f", fmt]
    if backing:
        cmd.extend(("-b", backing))
    cmd.append(path)
    if size is not None:
        cmd.append(str(size))
    subprocess.check_call(cmd)


def convert(src, dst, src_fmt, dst_fmt, progress=False):
    cmd = ["qemu-img", "convert", "-f", src_fmt, "-O", dst_fmt]
    if progress:
        cmd.append("-p")
    cmd.extend((src, dst))
    subprocess.check_call(cmd)


def info(path):
    cmd = ["qemu-img", "info", "--output", "json", path]
    out = subprocess.check_output(cmd)
    return json.loads(out.decode("utf-8"))
