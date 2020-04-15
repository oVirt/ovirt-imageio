# ovirt-imageio
# Copyright (C) 2015-2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from . import configloader


class daemon:

    # Interval in seconds for checking termination conditions.
    poll_interval = 1.0

    # Buffer size in bytes for data operations. The default value seems
    # to give optimal throughput with both low end and high end storage,
    # using iSCSI and FC. Larger values may increase throughput
    # slightly, but may also decrease it significantly.
    buffer_size = 8388608

    # Daemon run directory. Runtime stuff like socket or profile information
    # will be stored in this directory.
    # This is configurable only for development purposes and is not expected to
    # be changed by the user, therefore it's not documented in example
    # configuration.
    run_dir = "/run/ovirt-imageio"

    # The daemon is started under root be able to access files owned by root
    # and bind to privileged ports. Once started, privileges are dropped.
    # In the tests we want to run all the tests under the root and need an
    # option how to switch it off.
    # This is configurable only for development purposes and is not expected to
    # be changed by the user, therefore it's not documented in example
    # configuration.
    drop_privileges = True

    # Username under which imageio daemon will be run.
    # This is configurable only for development purposes and is not expected to
    # be changed by the user, therefore it's not documented in example
    # configuration.
    user_name = "ovirtimg"

    # Group under which imageio daemon will be run.
    # This is configurable only for development purposes and is not expected to
    # be changed by the user, therefore it's not documented in example
    # configuration.
    group_name = "ovirtimg"


class tls:

    # Enable TLS. Note that without TLS transfer tickets and image data are
    # transferred in clear text.
    # TODO: update the documentation with instructions, how to create
    # certificate files and link it in config file.
    enable = True

    # Private key file.
    key_file = ""

    # Certificate file.
    cert_file = ""

    # CA certificate file.
    ca_file = ""

    # Enable TLSv1.1, for legacy user applications that do not support
    # TLSv1.2.
    enable_tls1_1 = False


class remote:

    # Remote service interface. Use empty string to listen on any
    # interface.
    host = ""

    # Remote service port. Changing this value require change in the
    # firewall rules on the host, and changing this value in engine
    # configuration.
    port = 54322


class local:

    # Enable local service.
    enable = True

    # Local service unix socket for accessing images locally.
    socket = "\u0000/org/ovirt/imageio"


class control:

    # Transport be used to communicate with control service socket.
    # Can be either "tcp" or "unix". If "unix" is used, communication will
    # be done over UNIX socket which path is specified in "socket" option.
    # In case of TCP transport, you must specify the port using "port" option.
    # Preferred transport is unix as has better security - only users in
    # ovirtimg group can read/write into the socket.
    transport = "unix"

    # Control service socket path. This socket is used to control the
    # daemon and must be accessible only to the program controlling the
    # daemon.
    socket = "/run/ovirt-imageio/sock"

    # Control service port when run over TCP. Changing this value require
    # changing this value in engine configuration.
    # The default value is not set. If you want to use TCP transport, you
    # must specify port.
    port = -1


class profile:

    # Filename for storing profile data. Profiling requires the "yappi"
    # package. Version 0.93 is recommended for best performance.
    filename = "/run/ovirt-imageio/profile"


class Config:

    def __init__(self):
        self.daemon = daemon()
        self.tls = tls()
        self.remote = remote()
        self.local = local()
        self.control = control()
        self.profile = profile()


def load(files):
    cfg = Config()
    configloader.load(cfg, files)
    return cfg
