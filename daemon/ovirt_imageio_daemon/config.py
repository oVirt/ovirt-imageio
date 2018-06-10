# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.


class daemon:

    # Directory where vdsm pki files are stored.
    pki_dir = "/etc/pki/vdsm"

    # Interval in seconds for checking termination conditions.
    poll_interval = 1.0

    # Buffer size in bytes for data operations. Typically larger value
    # improve throughput and decrease cpu time.
    buffer_size = 1048576


class images:

    # Image service interface. Use empty string to listen on any
    # interface.
    host = ""

    # Image service port. Changing this value require change in the
    # firewall rules on the host, and changing this value in engine
    # configuration.
    port = 54322

    # Unix socket for accessing images locally.
    socket = "\0/org/ovirt/imageio"


class tickets:

    # tickets service socket path. This socket is used to control the
    # daemon and must be accessible only to the program controlling the
    # daemon.
    socket = "/run/vdsm/ovirt-imageio-daemon.sock"
