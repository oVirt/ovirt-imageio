# Image I/O flows

**WARNING: this document is outdated.**

## Upload image flow

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


### Pausing an upload

Pausing an upload only stops the monitoring.
The ticket remains in the daemon's cache and its timeout continues to
decrease.
If the client stops transferring data without pausing the transfer, the
monitoring continues.


### Resuming upload

- The client asks the backend to resume the upload.
- The backend extends the ticket if it's about to expire or has already
  expired.
- The client continues to send the image data to ovirt-imageio-proxy.


## Download image flow

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


### Resuming download

- To resume a download send a range request (use Range header) specifying
  the start offset to download and optionaly the last byte requested.
  This is already implemented in browsers and tools like wget.
- If the download ticket expires, the backend extends it.


## Monitoring upload and download

- Monitoring a transfer starts when the client begins to transfer data
  to the proxy.
- The backend monitors the progress of a transfer periodically by
  sending a Host.get_image_ticket request to Vdsm with the ticket id.
- Vdsm sends a ```GET``` request with the URL ```/tickets/<ticket id>```
  to ovirt-imageio-daemon and returns the json response.
- The backend updates the transfer's progress and state in the DB using
  the ticket's status.
- The UI updates the progress bar by reading the up to date status from
  the database.
- When the transfer is over, the monitoring ends.
- The following are responsible for finalizing the transfer:
  * The client when transferring via the SDK.
  * When uploading via the engine's UI, the UI itself.
  * When downloading via the engine's UI, the backend compares between
    the image size and the amount of bytes the daemon has transferred.
    If they are equal and no transfer operations exist, the backend
    finalizes the download itself.


## Image Session Flow (via Engine WebAdmin)

This illustrates the role the proxy plays in a typical image upload
operation.  The client is the Engine UI via a browser.

- Client initiates an upload via the UI.
- Engine creates an access token for ovirt-imageio-proxy and a
  session token for ovirt-image-daemon.
- Engine asks Vdsm to prepare the image.
- Engine asks Vdsm to add a ticket allowing access to the image.
- Vdsm adds the ticket to ovirt-imageio-daemon.
- Engine returns the signed access token and ovirt-imageio-proxy
  connection info to the client.  The access token contains the
  session token and image ovirt-imageio-daemon connection details.
- Client authenticates with ovirt-imageio-proxy using the signed
  access token.
- ovirt-imageio-proxy decodes and verifies the signed token, creates
  a session, and stores the token contents in the session state.
- ovirt-imageio-proxy returns the session id to the client.
- Client performs image operations with ovirt-imageio-proxy, using
  the ovirt-imageio-daemon session token and ovirt-imageio-proxy
  session id.
- ovirt-imageio-proxy relays image operations to ovirt-imageio-daemon
  using the session token.
- When the client is done, it asks Engine to finalize the operation.
- Engine ask vdsm to revoke the ticket.
- Vdsm deletes session from ovirt-imageio-daemon.
- Engine ask vdsm to tear down the image.
- Image verification and other necessary finalization actions
  involving Vdsm and Engine are performed.

Tickets are ephemeral; a client needs to ask Engine to renew the
ticket from time to time, otherwise a ticket will expire and the
ongoing image operations will be aborted.

Tickets are not persisted.  In case of an ovirt-imageio-proxy crash
or reboot, Engine will provide a new ticket and possibly point the
client to another host to continue the operation.
