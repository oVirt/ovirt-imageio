# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Helpers for working with oVirt python SDK.
"""

import logging
import time

import ovirtsdk4 as sdk
from ovirtsdk4 import types

log = logging.getLogger("ovirt")

# Image transfer constants.
DOWNLOAD = types.ImageTransferDirection.DOWNLOAD
RAW = types.DiskFormat.RAW
COW = types.DiskFormat.COW
ISO = types.DiskContentType.ISO
DATA = types.DiskContentType.DATA


class Repr:
    """
    Helper for logging sdk objects in a useful way.
    """

    def __init__(self, obj, key="id"):
        self.obj = obj
        self.key = key

    def __repr__(self):
        if self.obj is None:
            return "None"

        typename = type(self.obj).__name__
        value = getattr(self.obj, self.key, None)
        return f"{typename}({self.key}={value!r})"


def connect(args):
    return sdk.Connection(
        url=f"{args.engine_url}/ovirt-engine/api",
        username=args.username,
        password=args.password,
        ca_file=args.cafile,
        log=log,
        debug=args.log_level == "debug")


def find_disk(con, disk_id):
    service = _disk_service(con, disk_id)
    return service.get()


def add_disk(con, name, provisioned_size, sd_name, id=None,
             initial_size=None, sparse=True, enable_backup=True,
             content_type=DATA, format=COW):
    """
    Add a new disk to the storage domain, based on the source image
    information provided.

    Arguments:
        con (ovirtsdk4.Connection): connection to oVirt engine
        name (str): New disk alias.
        provisioned_size (int): Provisioned size of the new disk.
        sd_name (str): Storage Domain name that will contain the new disk.
        id (str): ID of the new disk to be added. By default oVirt creates
            a new UUID when not specified.
        initial_size (int): Initial size of the new disk.
        sparse (bool): New disk is sparse.
        enable_backup (bool): Disk can be used for incremental backups.
        content_type (ovirtsdk4.types.DiskContentType): Content type for the
            new disk.
        format (ovirtsdk4.types.DiskFormat): Format of the new disk.

    Returns:
        ovirtsdk4.types.Disk
    """
    log.info(
        "Adding disk name=%s provisioned_size=%s sd_name=%s id=%s "
        "initial_size=%s sparse=%s enable_backup=%s, content_type=%s "
        "format=%s", name, provisioned_size, sd_name, id, initial_size,
        sparse, enable_backup, content_type, format)

    disks_service = con.system_service().disks_service()
    disk = disks_service.add(
        disk=types.Disk(
            id=id,
            name=name,
            content_type=content_type,
            description='Uploaded by ovirt-img',
            format=format,
            initial_size=initial_size,
            provisioned_size=provisioned_size,
            sparse=sparse,
            backup=types.DiskBackup.INCREMENTAL if enable_backup else None,
            storage_domains=[
                types.StorageDomain(
                    name=sd_name
                )
            ]
        )
    )
    _wait_for_disk(con, disk.id)
    return disk


def find_storage_domain(con, disk):
    service = _storage_domain_service(con, disk.storage_domains[0].id)
    return service.get()


def find_host(con, sd_name):
    """
    Check if we can perform a transfer using the local host and return a
    host instance. Return None if we cannot use this host.

    Using the local host for an image transfer allows optimizing the
    connection using unix socket. This speeds up the transfer
    significantly and minimizes the network bandwidth.

    However using the local host is possible only if:
    - The local host is an oVirt host
    - The host is Up
    - The host is in the same DC of the storage domain

    Consider this setup:

        laptop1

        dc1
            host1 (down)
            host2 (up)
            sd1
                disk1

        dc2
            host3 (up)
            sd2
                disk2

    - If we run on laptop1 we cannot use the local host for any
      transfer.
    - If we run on host1, we cannot use the local host for any transfer.
    - If we run on host2, we can use use host2 for transferring disk1.
    - If we run on host3, we can use host3 for transferring disk2.

    Arguments:
        con (ovirtsdk4.Connection): Connection to ovirt engine
        sd_name (str): Storage domain name

    Returns:
        ovirtsdk4.types.Host
    """

    # Try to read this host hardware id.

    try:
        with open("/etc/vdsm/vdsm.id") as f:
            vdsm_id = f.readline().strip()
    except FileNotFoundError:
        log.debug("Not running on oVirt host, using any host")
        return None
    except OSError as e:
        # Unexpected error when running on ovirt host. Since choosing a
        # host is an optimization, log and continue.
        log.warning("Cannot read /etc/vdsm/vdsm.id, using any host: %s", e)
        return None

    log.debug("Found host hardware id: %s", vdsm_id)

    # Find the data center by storage domain name.

    system_service = con.system_service()
    data_centers = system_service.data_centers_service().list(
        search=f'storage.name={sd_name}',
        case_sensitive=True)
    if len(data_centers) == 0:
        raise RuntimeError(
            f"Storage domain {sd_name} is not attached to a DC")

    data_center = data_centers[0]
    log.debug("Found data center: %s", data_center.name)

    # Validate that this host is up and in data center.

    hosts_service = system_service.hosts_service()
    query = f"hw_id={vdsm_id} and datacenter={data_center.name} and status=Up"
    hosts = hosts_service.list(search=query, case_sensitive=True)
    if len(hosts) == 0:
        log.debug(
            "Cannot use host with hardware id %s, host is not up, or does "
            "not belong to data center %s",
            vdsm_id, data_center.name)
        return None

    host = hosts[0]
    log.debug("Using host id %s", host.id)

    return host


def create_transfer(
        con, image, direction=types.ImageTransferDirection.UPLOAD, host=None,
        backup=None, inactivity_timeout=None, timeout=60, shallow=None,
        timeout_policy=types.ImageTransferTimeoutPolicy.CANCEL):
    """
    Create an image transfer for upload or download.

    Arguments:
        con (ovirtsdk4.Connection): connection to ovirt engine
        image (ovirtsdk4.types.Disk | ovirtsdk4.types.DiskSnapshot): The image
            to transfer.
        direction (ovirtsdk4.typles.ImageTransferDirection): transfer
            direction (default UPLOAD)
        host (ovirtsdk4.types.Host): host object that should perform the
            transfer. If not specified engine will pick a random host.
        backup (ovirtsdk4.types.Backup): When downloading backup, the
            backup object owning the disks.
        inactivity_timeout (int): Number of seconds engine will wait for
            client activity before pausing the transfer. If not set, use
            engine default value.
        timeout (float, optional): number of seconds to wait for transfer
            to become ready.
        shallow (bool): Download only the specified image instead of the
            entire image chain. When downloading a disk transfer only
            the active disk snapshot data. When downloading a disk
            snapshot, transfer only the specified disk snaphost data.
        timeout_policy (ovirtsdk4.types.ImageTransferTimeoutPolicy): the
            action to take after inactivity timeout.

    Returns:
        ovirtsdk4.types.ImageTransfer in phase TRANSFERRING
    """
    log.info(
        "Creating transfer image=%s direction=%s host=%s backup=%s "
        "shallow=%s timeout_policy=%s",
        Repr(image, "id"),
        direction,
        Repr(host, "name"),
        Repr(backup, "id"),
        shallow,
        timeout_policy)

    start = time.monotonic()
    deadline = start + timeout

    transfer = types.ImageTransfer(
        host=host,
        direction=direction,
        backup=backup,
        inactivity_timeout=inactivity_timeout,
        timeout_policy=timeout_policy,
        format=RAW,
        shallow=shallow)

    if isinstance(image, types.Disk):
        transfer.disk = image
    elif isinstance(image, types.DiskSnapshot):
        transfer.snapshot = image
    else:
        raise ValueError(f"Cannot transfer {Repr(image)}")

    transfers_service = con.system_service().image_transfers_service()
    transfer = transfers_service.add(transfer)

    # At this point the transfer owns the disk and will delete the disk
    # if the transfer is canceled, or if finalizing the transfer fails.

    transfer_service = _transfer_service(con, transfer.id)

    while True:
        try:
            transfer = transfer_service.get()
        except sdk.NotFoundError:
            # The system has removed the disk and the transfer.
            raise RuntimeError(f"Transfer {transfer.id} was removed")

        if transfer.phase == types.ImageTransferPhase.FINISHED_FAILURE:
            # The system will remove the disk and the transfer soon.
            raise RuntimeError(f"Transfer {transfer.id} has failed")

        if transfer.phase == types.ImageTransferPhase.PAUSED_SYSTEM:
            transfer_service.cancel()
            raise RuntimeError(f"Transfer {transfer.id} was paused by system")

        if transfer.phase == types.ImageTransferPhase.TRANSFERRING:
            break

        if transfer.phase != types.ImageTransferPhase.INITIALIZING:
            transfer_service.cancel()
            raise RuntimeError(
                f"Unexpected transfer {transfer.id} phase {transfer.phase}")

        if time.monotonic() > deadline:
            log.info("Cancelling transfer %s", transfer.id)
            transfer_service.cancel()
            raise RuntimeError(
                f"Timed out waiting for transfer {transfer.id}")

        time.sleep(1)

    # Log the transfer host name. This is very useful for
    # troubleshooting.
    transfer.host = con.follow_link(transfer.host)

    log.info("Transfer %r ready on host %r in %.1f seconds",
             transfer.id, transfer.host.name, time.monotonic() - start)

    return transfer


def cancel_transfer(con, transfer):
    """
    Cancel a transfer and remove the disk for upload transfer.

    There is not need to cancel a download transfer, it can always be
    finalized.
    """
    log.info("Cancelling transfer %r", transfer.id)
    transfer_service = _transfer_service(con, transfer.id)
    transfer_service.cancel()


def finalize_transfer(con, transfer, disk, timeout=300):
    """
    Finalize a transfer, making the transfer disk available.

    If finalizing succeeds: the disk status will change to OK and
    transfer's phase will change to FINISHED_SUCCESS.
    On upload errors: the disk status will change to ILLEGAL, transfer's
    phase will change to FINISHED_FAILURE and the disk will be removed.
    In both cases the transfer entity:
     a. prior to 4.4.7: is removed shortly after the command finishes
     b. 4.4.7 and later: stays in the database and is cleaned by the
        dedicated thread after a few minutes.

    If oVirt fails to finalize the transfer, transfer's phase will
    change to PAUSED_SYSTEM. In this case the disk's status will change
    to ILLEGAL and it will not be removed.

    When working with oVirt 4.4.7 and later, it is enough to poll the
    image transfer. However with older versions the transfer entity is
    removed from the database after the the command finishes and before
    we can retrieve the final transfer status. Thus the API returns a
    404 error code. In that case we need to check for the disk status.

    For more info see:
    - http://ovirt.github.io/ovirt-engine-api-model/4.4/#services/image_transfer
    - http://ovirt.github.io/ovirt-engine-sdk/master/types.m.html#ovirtsdk4.types.ImageTransfer

    Arguments:
        con (ovirtsdk4.Connection): connection to ovirt engine
        transfer (ovirtsdk4.types.ImageTransfer): image transfer to
            finalize
        disk (ovirtsdk4.types.Disk): disk associated with the image
            transfer
        timeout (float, optional): number of seconds to wait for
            transfer to finalize.
    """  # noqa E501 (long line)
    log.info("Finalizing transfer %r", transfer.id)

    start = time.monotonic()

    transfer_service = _transfer_service(con, transfer.id)

    transfer_service.finalize()
    while True:
        time.sleep(1)
        try:
            transfer = transfer_service.get()
        except sdk.NotFoundError:
            # Old engine (< 4.4.7): since the transfer was already
            # deleted from the database, we can assume that the disk
            # status is already updated, so we can check it only once.
            disk_service = _disk_service(con, disk.id)
            try:
                disk = disk_service.get()
            except sdk.NotFoundError:
                # Disk verification failed and the system removed the
                # disk.
                raise RuntimeError(
                    f"Transfer {transfer.id} failed: disk {disk.id} was "
                    "removed")

            if disk.status == types.DiskStatus.OK:
                break

            raise RuntimeError(
                    f"Transfer {transfer.id} failed: disk {disk.id} status :"
                    f"{disk.status}")

        log.debug("Transfer %r in phase %r", transfer.id, transfer.phase)

        if transfer.phase == types.ImageTransferPhase.FINISHED_SUCCESS:
            break

        if transfer.phase == types.ImageTransferPhase.FINISHED_FAILURE:
            raise RuntimeError(f"Transfer {transfer.id} failed")

        if time.monotonic() > start + timeout:
            raise RuntimeError(
                f"Timed out waiting for transfer {transfer.id} to finalize, "
                f"transfer is {transfer.phase}")

    log.info("Transfer %r finalized in %.1f seconds",
             transfer.id, time.monotonic() - start)


def _wait_for_disk(con, disk_id):
    log.info("Waiting for disk %s", disk_id)
    timeout = 120
    start = time.monotonic()
    deadline = start + timeout
    disk_service = _disk_service(con, disk_id)
    while True:
        time.sleep(1)
        if time.monotonic() > deadline:
            raise RuntimeError(f"Timeout reached waiting for disk {disk_id}")

        if disk_service.get().status == types.DiskStatus.OK:
            break


def _disk_service(con, disk_id):
    return con.system_service().disks_service().disk_service(disk_id)


def _storage_domain_service(con, storage_domain_id):
    return (con.system_service()
            .storage_domains_service()
            .storage_domain_service(storage_domain_id))


def _transfer_service(con, transfer_id):
    return (con.system_service()
            .image_transfers_service()
            .image_transfer_service(transfer_id))
