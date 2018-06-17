# UNIX socket support

## Use cases

- Importing VM disk using virt-v2v: improves transferring multiple disks
  concurrently using less resources.
- Backup vendors: improves backup/restore solution scalability.

## An example flow:

### 1. Select a running host from the data center

A user program selects a running host for image transfers using the API.
The transfers can be distributed over all hosts in a data center to
distribute the I/O.


### 2. Find the host uuid

Currently the host uuid used in engine side is not kept on the host, but
we can find it using the host hardware uuid at /etc/vdsm/vdsm.id.

    with open("/etc/vdsm/vdsm.id") as f:
        vdsm_id = f.readline().strip()

    hosts_service = connection.system_service().hosts_service()
    hosts = hosts_service.list(
        search='hw_uuid=%s' % vdsm_id,
        case_sensitive=False,
    )


### 3. Start an image transfer using the found host uuid

After finding the host uuid we can start an image transfer on that
specific host.

    transfers_service = system_service.image_transfers_service()
    transfer = transfers_service.add(
        types.ImageTransfer(
            disk=types.Disk(id='123'),
            host=types.Host(id=hosts[0].id)
        )
    )


### 4. Find the unix socket address using OPTIONS on the transfer_url

The first request to the server should be OPTIONS /images/ticket-uuid
(to find the features supported by the server, e.g. "zero").
This will also expose the unix socket address.

    OPTIONS /images/ticket-uuid
    ..
    {
        "features": ["zero", "flush"],
        "unix_socket": "\0/org/ovirt/imageio"
    }

### 5. Transfer the data using the unix socket

The user program needs to use a modified httplib.HTTPConnection to
connect using socket address. Then it can use standard HTTPConnection
APIs to perform the transfers.

We have example implementation here:
https://github.com/oVirt/ovirt-imageio/blob/master/daemon/ovirt_imageio_daemon/uhttp.py

The following pypi package can also be used:
https://pypi.org/project/uhttplib/
