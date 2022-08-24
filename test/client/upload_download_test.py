# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import contextlib
import logging
import os
import uuid
import subprocess
import sys

import pytest
import ovirtsdk4 as sdk

from ovirt_imageio._internal import qemu_img


OS_VERSION = 'fedora-36'

log = logging.getLogger("test")


class ClientError(Exception):
    pass


def create_disk(path, fmt, os_version=OS_VERSION):
    env = os.environ.copy()
    env['LIBGUESTFS_BACKEND'] = 'direct'
    cmd = ['virt-builder', os_version, '--format', fmt, '-o', path]
    subprocess.check_call(cmd, env=env)


def run_upload_disk(storage_domain, image, disk_id=None, log_level=None):
    # Make sure it runs with the same tox environment executable
    cmd = [sys.executable, './ovirt-img', 'upload-disk', '-c', 'test']
    cmd.extend(['-s', storage_domain])
    if log_level:
        cmd.extend(['--log-level', log_level])
    if disk_id:
        cmd.extend(['--disk-id', disk_id])
    cmd.append(image)
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise ClientError(f'Client Error: {exc}') from exc


def run_download_disk(disk_id, image, log_level=None):
    # Make sure it runs with the same tox environment executable
    cmd = [sys.executable, './ovirt-img', 'download-disk', '-c', 'test']
    if log_level:
        cmd.extend(['--log-level', log_level])
    cmd.extend([disk_id, image])
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise ClientError(f'Client Error: {exc}') from exc


def remove_disk(conf, sd_name, disk_id):
    connection = sdk.Connection(
        url=f'{conf["engine_url"]}/ovirt-engine/api',
        username=conf["username"],
        password=conf["password"],
        ca_file=conf["cafile"]
    )
    with contextlib.closing(connection):
        sd_service = connection.system_service().storage_domains_service()
        found_sd = sd_service.list(search=f'name={sd_name}')
        if not found_sd:
            raise RuntimeError(f"Couldn't find storage domain {sd_name}")

        sd = found_sd[0]
        sd_service = sd_service.storage_domain_service(sd.id)
        sd_service.disks_service().disk_service(disk_id).remove()


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_download(config, tmpdir, fmt):
    image = os.path.join(tmpdir, f'image.{fmt}')
    create_disk(image, fmt)
    test_config = config["tests"]["upload-download"]
    for sd_name in test_config.get("storage-domains", []):
        disk_id = str(uuid.uuid4())
        try:
            log.info("Upload %s image to SD %s", fmt, sd_name)
            run_upload_disk(sd_name, image, disk_id)
            down_img = os.path.join(tmpdir, f'downloaded.{fmt}')
            log.info("Download image %s", disk_id)
            run_download_disk(disk_id, down_img)
            log.info("Comparing images")
            qemu_img.compare(image, down_img)
        except ClientError as exc:
            log.error("%s", exc)
            # Skip disk cleanup if client failed
            return
        finally:
            remove_disk(config["common"], sd_name, disk_id)
