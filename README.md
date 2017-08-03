# ovirt-imageio


## Overview

ovirt-imageio enables uploading and downloading of disks using HTTPS.

The system contains these components:

- Engine - Engine UI starts image I/O operations, communicating with
  Engine backend and ovirt-imageio-proxy.  Engine backend communicate
  with Vdsm on the host for preparing for I/O operations, monitoring
  operations, and cleaning up.  This part is developed in the
  ovirt-engine project.  See https://github.com/ovirt/ovit-engine

- Vdsm - prepares a host for image I/O operations, provides monitoring
  APIs for monitoring operations progress, and cleans up when the
  operation is done. Vdsm communicates with host's ovirt-imageio-daemon.
  This part is developed in the vdsm project.  See
  https://github.com/ovirt/vdsm

- Proxy - allowing clients without access to the host network to perform
  I/O disk operations. This part is developed in this project.

- Daemon - expose images over HTTPS, allowing clients to read or write
  to images. This part is developed in this project.


## Image I/O flows


### Upload image flow

In this flow, client can be engine UI (webadmin), or user program using
the oVirt REST API or oVirt SDK.

- Client asks the backend to start an upload session.
- Engine creates signed ticket with the upload details.
- Engine creates a new disk
- Engine asks Vdsm to prepare the image for this disk.
- Engine asks Vdsm to add a upload ticket
- Vdsm adds the upload ticket to ovirt-imageio-daemon.
- Engine returns signed ticket
- Client authenticates with ovirt-imageio-proxy using the signed ticket.
- Client sends image data to imageio proxy
- ovirt-imageio-proxy relay image data to ovirt-imageio-daemon on the
  host, using the ticket id.
- ovirt-imageio-daemon writes image data to storage.
- When client is done, it asks the backend to end the session.
- Engine asks vdsm to revoke the upload ticket.
- Vdsm deletes the upload ticket from ovirt-imageio-daemon.
- Engine asks Vdsm to verify the upload.
- Engine asks vdsm to tear down the image.


#### Monitoring upload

The UI is updating the backend about sent data each time a chunk is sent
to ovirt-imageio-proxy.

Write me: how upload progress is managed by backend.


#### Resuming upload

Write me


### Download image flow

In this flow, client can be engine UI (webadmin), or user program using
the oVirt REST API or oVirt SDK.

- Client asks the backend to start a download session.
- Engine creates signed ticket with the download details.
- Engine asks Vdsm to prepare the image for this disk.
- Engine asks Vdsm to add a download ticket.
- Vdsm adds the download ticket to ovirt-imageio-daemon.
- Engine returns signed ticket.
- Client authenticates with ovirt-imageio-proxy sending ```POST```
  request to ```/sessions```.
- ovirt-imageio-proxy create download session with the signed ticket and
  return the session id in the ```session-id``` header.
- Client open download URL from ovirt-imageio-proxy using ```GET```
  request to ```/images/ticket_id?session_id=XXXYYY```
- Client ask backend to start monitoring the dowload
- The browser fetches the image data from the ovirt-imageio-proxy and
  store it to the user selected filename.
- ovirt-imageio-proxy fetch image data from ovirt-imageio-daemon on the
  host, using the ticket id.
- ovirt-imageio-daemon read image from storage.
- When the download is done, backend stops monitoring the download operation
- The backend deletes the download session from ovirt-imageio-proxy.
- Engine asks vdsm to revoke the download ticket.
- Vdsm deletes the download ticket from ovirt-imageio-daemon.
- Engine asks vdsm to tear down the image.


#### Monitoring download

The backend will monitor the download progress every 4 seconds. On each
monitoring cycle:
- backend send Host.get_image_ticket request to Vdsm, with the ticket
  id.
- Vdsm send ```GET``` request to ovirt-imageio-daemon to
  ```/tickets/<ticket id>``` and return the json response.
- backend update the operation progress and state using the ticket
  status.
- UI will update the progress bar using the upadated state in the
  database.


#### Resuming download

- To resume a download send a range request (use Range header) specifying
  the start offset to download and optionaly the last byte requested.
  This is already implemented in browsers and tools like wget.
- If the download ticket expires, the user have to restart the download.


## Tickets

Tickets are ephemeral; A client needs to request Engine to renew the
ticket from time to time, otherwise a ticket will expire and the
ongoing image operations will be aborted.

Tickets are not persisted. In case of ovirt-imageio-daemon crash or
reboot, Engine will provide a new ticket and possibly point client to
another host to continue the operation.
