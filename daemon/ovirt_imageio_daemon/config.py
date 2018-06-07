# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.


class daemon:

    # Directory where vdsm pki files are stored (default /etc/pki/vdsm).
    pki_dir = "/etc/pki/vdsm"

    # Interval in seconds for checking termination conditions (defalt 1.0).
    poll_interval = 1.0

    # Buffer size in bytes for data operations.  Typically, larger value
    # improve throughput and decrease cpu time (default 1048576).
    buffer_size = 1048576


class images:

    # Image service interface (default "").
    host = ""

    # Image service port.
    port = 54322

    # Unix socket for accessing images locally.
    socket = "\0/org/ovirt/imageio"


class tickets:

    # tickets service socket path
    socket = "/var/run/vdsm/ovirt-imageio-daemon.sock"
