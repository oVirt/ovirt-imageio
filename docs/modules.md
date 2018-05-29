# ovirt-imageio-daemon

ovirt-imageio-daemon provides direct access to oVirt disks using HTTPS
protocol.  Together with ovirt-imageio-proxy, it allows uploading a disk
image directly into an oVirt disk, downloading an oVirt disk, or
performing random I/O.

The goal is to keep ovirt-imageio-daemon simple as possible. We use a
single protocol (HTTP) for everything, and avoid dependencies on Vdsm.

This daemon provides these services:

- images service    read and write data to/from images. This service is
                    available via HTTPS on port 54322. This service is
                    accessed by ovirt-imageio-proxy or directly by
                    clients.

- tickets service   manage tickets authorizing images service
                    operations. Available localy via HTTP over unix
                    domain socket. Vdsm is using this service to add,
                    remove, and extend tickets per ovirt-engine
                    request.

- progress service  report progress for ongoing images operations.
                    Available locally via HTTP over unix domain socket.
                    Vdsm will access this to report progress to Engine.


# ovirt-imageio-proxy

The oVirt ImageIO Proxy provides a proxy server allowing clients to
perform I/O with VM disk images and ISOs located within the oVirt
virtualization environment.

The proxy provides the following service:

- images service    read and write data to/from the imageio daemon.
                    By default this service is available via HTTPS
                    on port 54323.  This service is accessed by
                    clients wishing to transfer data to/from images
                    that do not wish to transfer directly via the
                    daemon.


# ovirt-imageio-common

ovirt-imageio-common provides common functionality for ovirt projects related to
ovirt images- such as VDSM, and other ovirt-imageio projects- "ovirt-imageio-daemon"
and "ovirt-imageio-proxy".

This project includes the following common modules:

## web.py

A web application infrastructure for web application projects to use. the module
supplies an "Application" class, which can get multiple handlers for HTTP method
calls. This module is being used by ovirt-imageio-daemon and ovirt-imageio-proxy: web
applications for ovirt images transferring. (See README file in each project for
more details)

## directio.py

A module for doing direct I/O from a stream source to a destination file in the
system. This module is being used by VDSM for fetching VM disks from a remote source
using libvirt's API, and by ovirt-imageio-daemon for I/O of oVirt's disks by its clients.
