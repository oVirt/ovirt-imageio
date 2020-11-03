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

    # Maximum number of connections for same /image/ticket-id URL. Using more
    # connections improves throughput of a single image transfer. When
    # transferring images concurrently, using more connections per transfer may
    # decrease throughput.
    max_connections = 8

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

    # Private key file, must not be empty.
    key_file = ""

    # Certificate file, must not be empty.
    cert_file = ""

    # CA certificate file. Empty value is valid, meaning use the host trusted
    # CAs.
    ca_file = ""

    # Enable TLSv1.1, for legacy user applications that do not support
    # TLSv1.2.
    enable_tls1_1 = False


class backend_file:

    # Buffer size in bytes for reading and writing using the file backend. The
    # default value seems to give optimal throughput with both low end and high
    # end storage, using iSCSI and FC. Larger values may increase throughput
    # slightly, but may also decrease it significantly.
    # TODO: Tested with single writer, needs testing with multiple readers.
    buffer_size = 8 * 1024**2


class backend_http:

    # CA certificate file to be used with HTTP backend. Empty value is valid,
    # meaning uses CA file configured in TLS section.
    # This option has to be used when the daemon serving as a proxy use
    # different CA than daemon serving storage backend. In most of the cases
    # these CAs are the same and one would use same value as configured in
    # tls.ca_file option.
    ca_file = ""

    # Buffer size in bytes for handling proxy requests. The default value was
    # copied form the file backend.
    # TODO: Needs testing with multiple readers and writers.
    buffer_size = 8 * 1024**2


class backend_nbd:

    # Buffer size in bytes when reading and writing to the nbd backend. The
    # default value was copied form the file backend.
    # TODO: Needs testing with multiple readers and writers.
    buffer_size = 8 * 1024**2


class remote:

    # Remote service interface. Use "::" to listen on any interface on both
    # IPv4 and IPv6. To listen only on IPv4, use "0.0.0.0".
    host = "::"

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

    # Determines if IPv4 address should be preferred when the address of
    # control service is resolved. This option allows compatibility with java
    # clients which may prefer IPv4 address and don't try other addresses on
    # dual stack system.
    prefer_ipv4 = True

    # Number of seconds to wait when removing a ticket. If ticket cannot be
    # removed within this timeout, the request will fail with "409 Conflict",
    # and the user need to retry the request again. A ticket can be removed
    # only when the number of connections using the ticket is zero.
    remove_timeout = 60


class profile:

    # Filename for storing profile data. Profiling requires the "yappi"
    # package. Version 0.93 is recommended for best performance.
    filename = "/run/ovirt-imageio/profile"


# Logger configuration.
# See Python logging documentation for details how to configure loggers.

class loggers:
    keys = "root"


class handlers:
    keys = "logfile"


class formatters:
    keys = "long"


class logger_root:
    level = "INFO"
    handlers = "logfile"
    propagate = 0


class handler_logfile:
    keyword__class = "logging.handlers.RotatingFileHandler"
    args = '("/var/log/ovirt-imageio/daemon.log",)'
    kwargs = '{"maxBytes": 20971520, "backupCount": 10}'
    level = "DEBUG"
    formatter = "long"


class handler_stderr:
    keyword__class = "logging.StreamHandler"
    args = "()"
    level = "DEBUG"
    formatter = "long"


class formatter_long:
    format = ("%(asctime)s %(levelname)-7s (%(threadName)s) [%(name)s] "
              "%(message)s")


class Config:

    def __init__(self):
        # Daemon config.

        self.daemon = daemon()
        self.tls = tls()
        self.backend_file = backend_file()
        self.backend_http = backend_http()
        self.backend_nbd = backend_nbd()
        self.remote = remote()
        self.local = local()
        self.control = control()
        self.profile = profile()

        # Logger config.

        self.loggers = loggers()
        self.handlers = handlers()
        self.formatters = formatters()
        self.logger_root = logger_root()
        self.handler_logfile = handler_logfile()
        self.handler_stderr = handler_stderr()
        self.formatter_long = formatter_long()


def load(files):
    cfg = Config()
    configloader.load(cfg, files)
    return cfg


def to_dict(config):
    return configloader.to_dict(config)
