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

    # Buffer size in bytes for data operations. The default value seems
    # to give optimal throughput with both low end and high end storage,
    # using iSCSI and FC. Larger values may increase throughput
    # slightly, but may also decrease it significantly.
    buffer_size = 8388608


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
