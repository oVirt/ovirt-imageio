# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
"""
nbd - Network Block Device
"""

from __future__ import absolute_import

import itertools
import logging
import re
import socket
import struct

import six

from . import util

# Matcher for NBD Unix URL path.
# nbd:unix:path[:exportname=name]
UNIX_URL_PATH = re.compile(
    r"unix:(?P<path>/[^:]+)(:?:exportname=(?P<name>.*))?")

# Matcher for NBD TCP URL path.
# nbd:host:port[:exportname=name]
TCP_URL_PATH = re.compile(
    r"(?P<host>.+):(?P<port>\d+)(:?:exportname=(?P<name>.*))?")

# Magic numbers.
NBDMAGIC = 0x4e42444d41474943
IHAVEOPT = 0x49484156454F5054
OPTION_REPLY_MAGIC = 0x3e889045565a9
NBD_REQUEST_MAGIC = 0x25609513
NBD_SIMPLE_REPLY_MAGIC = 0x67446698

# Flags
NBD_FLAG_FIXED_NEWSTYLE = 1
NBD_FLAG_NO_ZEROES = 2
NBD_FLAG_C_FIXED_NEWSTYLE = 1
NBD_FLAG_C_NO_ZEROES = 2

# Transmission flags
NBD_FLAG_HAS_FLAGS = (1 << 0)
NBD_FLAG_READ_ONLY = (1 << 1)
NBD_FLAG_SEND_FLUSH = (1 << 2)
NBD_FLAG_SEND_FUA = (1 << 3)
NBD_FLAG_ROTATIONAL = (1 << 4)
NBD_FLAG_SEND_TRIM = (1 << 5)
NBD_FLAG_SEND_WRITE_ZEROES = (1 << 6)
NBD_FLAG_SEND_DF = (1 << 7)
NBD_FLAG_CAN_MULTI_CONN = (1 << 8)
NBD_FLAG_SEND_RESIZE = (1 << 9)
NBD_FLAG_SEND_CACHE = (1 << 10)

# Options
NBD_OPT_ABORT = 2
NBD_OPT_GO = 7

# Replies
NBD_REP_ACK = 1
NBD_REP_INFO = 3

# NBD_INFO replies
NBD_INFO_EXPORT = 0
NBD_INFO_BLOCK_SIZE = 3

# Commands
NBD_CMD_READ = 0
NBD_CMD_WRITE = 1
NBD_CMD_DISC = 2
NBD_CMD_FLUSH = 3
NBD_CMD_WRITE_ZEROES = 6

# Error replies
ERR_BASE = 2**31
NBD_REP_ERR_UNSUP = ERR_BASE + 1
NBD_REP_ERR_POLICY = ERR_BASE + 2
NBD_REP_ERR_INVALID = ERR_BASE + 3
NBD_REP_ERR_PLATFORM = ERR_BASE + 4
NBD_REP_ERR_TLS_REQD = ERR_BASE + 5
NBD_REP_ERR_UNKNOWN = ERR_BASE + 6
NBD_REP_ERR_SHUTDOWN = ERR_BASE + 7
NBD_REP_ERR_BLOCK_SIZE_REQD = ERR_BASE + 8
NBD_REP_ERR_TOO_BIG = ERR_BASE + 9

ERROR_REPLY = {
    NBD_REP_ERR_UNSUP: (
        "The option sent by the client is unknown by this server "
        "implementation"),
    NBD_REP_ERR_POLICY: (
        "The option sent by the client is known by this server and "
        "syntactically valid, but server-side policy forbids the server to "
        "allow the option"),
    NBD_REP_ERR_INVALID: (
        "The option sent by the client is known by this server, but was "
        "determined by the server to be syntactically or semantically "
        "invalid"),
    NBD_REP_ERR_PLATFORM: (
        "The option sent by the client is not supported on the platform on "
        "which the server is running"),
    NBD_REP_ERR_TLS_REQD: (
        "The server is unwilling to continue negotiation unless TLS is "
        "initiated first"),
    NBD_REP_ERR_UNKNOWN: "The requested export is not available",
    NBD_REP_ERR_SHUTDOWN: (
        "The server is unwilling to continue negotiation as it is in the "
        "process of being shut down"),
    NBD_REP_ERR_BLOCK_SIZE_REQD: (
        "The server is unwilling to enter transmission phase for a given "
        "export unless the client first acknowledges (via "
        "NBD_INFO_BLOCK_SIZE) that it will obey non-default block sizing "
        "requirements"),
    NBD_REP_ERR_TOO_BIG: "The request or the reply is too large to process",
}

log = logging.getLogger("nbd")


class Error(Exception):
    pass


class OptionError(Error):
    fmt = ("Error negotiating option opt={self.opt} code={self.code} "
           "reason={self.reason}")

    def __init__(self, opt, code, reason):
        self.opt = opt
        self.code = code
        self.reason = reason

    def __str__(self):
        return self.fmt.format(self=self)


class OptionUnsupported(OptionError):
    fmt = "Option {self.option} is not supported: {self.reason}"
    code = NBD_REP_ERR_UNSUP

    def __init__(self, option, reason):
        self.option = option
        self.reason = reason


class UnixAddress(str):
    """
    A unix socket path with additioal methods to make it easier to handle both
    unix socket and TCP socket in the same code.

    Because we inherit from str, you can pass an instance to socket.connect()
    or socket.bind().
    """

    @property
    def transport(self):
        return "unix"

    @property
    def path(self):
        return str(self)

    def url(self, export=None):
        s = "nbd:unix:{}".format(self.path)
        if export:
            s += ":exportname=" + export
        return s


class TCPAddress(tuple):
    """
    A TCP socket 2 tuple (host, port) with additioal methods to make it easier
    to handle both unix socket and TCP socket in the same code.

    Because we inherit from tuple, you can pass an instance to socket.connect()
    or socket.bind().
    """

    def __new__(cls, host, port):
        if not isinstance(host, six.string_types):
            raise ValueError("Invalid host {!r}, expecting string value"
                             .format(host))
        if not isinstance(port, six.integer_types):
            raise ValueError("Invalid port {!r}, expecting integer value"
                             .format(port))
        return tuple.__new__(cls, (host, port))

    @property
    def transport(self):
        return "tcp"

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]

    def url(self, export=None):
        s = "nbd:{}:{}".format(self.host, self.port)
        if export:
            s += ":exportname=" + export
        return s


def open(url):
    """
    Open parsed NBD URL and return a connected Client instance.
    """
    address, name = _parse_url(url)
    return Client(address, name)


def _parse_url(url):
    """
    Parse url and return 2 tuple (address, name), or raise an Error.
    """
    if url.scheme != "nbd":
        raise Error("Unsupported URL scheme: {}".format(url))

    # First try the nice URL notation:
    # nbd://localhost:10809/sda
    # This notiation is less flexible but nicer for humans.
    # See https://qemu.weilnetz.de/doc/qemu-doc.html#disk_005fimages_005fnbd
    if ":" in url.netloc:
        host, port = url.netloc.rsplit(":", 1)
        # Use qemu semantics, removing leading "/".
        export = url.path.lstrip("/")
        return TCPAddress(host, int(port)), export

    # Next try to documented NBD URL notation. This notiation is more flexible
    # and can handle export names with leading "/".
    # - nbd:unix:path[:exportname=name]
    # - nbd:host:port[:exportname=name]
    # See https://qemu.weilnetz.de/doc/qemu-doc.html#Device-URL-Syntax
    if url.netloc == "":
        match = UNIX_URL_PATH.match(url.path)
        if match:
            d = match.groupdict()
            return UnixAddress(d["path"]), d["name"]

        match = TCP_URL_PATH.match(url.path)
        if match:
            d = match.groupdict()
            return TCPAddress(d["host"], int(d["port"])), d["name"]

    raise Error("Unsupported URL: {}".format(url))


# Client states

CONNECTING = 0
HANDSHAKE = 1
TRASMISSION = 2
CLOSED = 3


class Client(object):

    def __init__(self, address, export_name=None):
        if export_name is None:
            export_name = ""
        self.export_size = None
        self.transmission_flags = None

        # If a server does not advertise block size constraints, it should
        # support these values. It can also return reads and block status info
        # aligned to minimum block size.
        # https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
        # section #block-size-constraints

        self.minimum_block_size = 1
        self.preferred_block_size = 4096
        self.maximum_block_size = 32 * 1024**2

        self._counter = itertools.count()
        self._state = CONNECTING

        log.info("Connecting to %s %r", address, export_name)

        self._sock = self._connect(address)
        try:
            self._newstyle_handshake(export_name)
        except:
            self.close()
            raise

        log.info("Ready for transmission")

    def read(self, offset, length):
        handle = next(self._counter)
        self._send_command(NBD_CMD_READ, handle, offset, length)
        self._receive_simple_reply(handle)
        return self._receive(length)

    def readinto(self, offset, buf):
        handle = next(self._counter)
        self._send_command(NBD_CMD_READ, handle, offset, len(buf))
        self._receive_simple_reply(handle)
        return self._receive_into(buf)

    def write(self, offset, data):
        handle = next(self._counter)
        self._send_command(NBD_CMD_WRITE, handle, offset, len(data))
        self._send(data)
        self._receive_simple_reply(handle)

    def zero(self, offset, length):
        if self.transmission_flags & NBD_FLAG_SEND_WRITE_ZEROES == 0:
            raise Error("Server does not support NBD_CMD_WRITE_ZEROES")
        handle = next(self._counter)
        self._send_command(NBD_CMD_WRITE_ZEROES, handle, offset, length)
        self._receive_simple_reply(handle)

    def flush(self):
        # TODO: is this the best way to handle this?
        if self.transmission_flags & NBD_FLAG_SEND_FLUSH == 0:
            return
        handle = next(self._counter)
        self._send_command(NBD_CMD_FLUSH, handle, 0, 0)
        self._receive_simple_reply(handle)

    def close(self):
        if self._state in (HANDSHAKE, TRASMISSION):
            self._soft_disconnect()
        else:
            self._hard_disconnect()

    # Connecting to NBD server

    def _connect(self, address):
        """
        Connect to NBD server on address and return a connected socket, or
        raise socket.error.
        """
        if address.transport == "unix":
            return self._create_unix_connection(address)
        elif address.transport == "tcp":
            return self._create_tcp_connection(address)
        else:
            raise Error("Unsupported transport: {}".format(address))

    def _create_tcp_connection(self, address):
        """
        Enhanced version of socket.create_connection.

        Resolve DNS name to both AF_INET and AF_INET6 and will try to connect
        to all possible addresses.

        Set socket option TCP_NODELAY for improved latency.
        """
        sock = socket.create_connection(address)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except:
            sock.close()
            raise

        return sock

    def _create_unix_connection(self, address):
        """
        Like socket.create_connection() for unix socket.
        """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(address)
        except:
            sock.close()
            raise

        return sock

    # NBD fixed newstyle handshake

    def _newstyle_handshake(self, export_name):
        assert self._state == CONNECTING
        self._state = HANDSHAKE

        # Initial handshake.
        nbd_magic, cliserv_magic, server_flags = self._receive_struct("!QQH")

        if nbd_magic != NBDMAGIC:
            raise Error("Bad nbd magic {:x}, expecting {:x}"
                        .format(nbd_magic, NBDMAGIC))

        if cliserv_magic != IHAVEOPT:
            raise Error("Server does not support newstyle negotiation "
                        "[magic={:x} expected={:x}]"
                        .format(cliserv_magic, IHAVEOPT))

        log.debug("Received server flags: %x", server_flags)
        if not server_flags & NBD_FLAG_FIXED_NEWSTYLE:
            raise Error("Server does not support fixed newstyle negotiation")

        self._send_client_flags(NBD_FLAG_C_FIXED_NEWSTYLE)

        # Options haggling.
        self._negotiate_go_option(export_name)

        self._state = TRASMISSION

    def _send_client_flags(self, flags):
        log.debug("Sending client flags: %x:", flags)
        self._send_struct("!I", flags)

    # GO Option

    def _negotiate_go_option(self, export_name):
        # Here we can announce that we can honour server block size constraints
        # by adding NBD_INFO_BLOCK_SIZE information request. If we do this we
        # MUST abide by the block size constraints received. If we don't we are
        # allowed to send unaligned requests.
        # https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
        # section #block-size-constraints

        data = self._format_go_option_data(export_name)
        self._send_option(NBD_OPT_GO, data)

        while True:
            reply, length = self._receive_option_reply(NBD_OPT_GO)

            if reply in ERROR_REPLY:
                self._handle_option_error(NBD_OPT_GO, reply, length)

            if reply == NBD_REP_ACK:
                if self.export_size is None or self.transmission_flags is None:
                    raise Error("Server did not send export info")
                break

            if reply != NBD_REP_INFO:
                raise Error("Unexpected reply {}, expecting info reply {}"
                            .format(reply, NBD_REP_INFO))

            if length < 2:
                raise Error("Invalid short reply {}".format(length))

            info = self._receive_struct("!H")[0]
            length -= 2

            if info == NBD_INFO_EXPORT:
                self._receive_export_info(length)
            elif info == NBD_INFO_BLOCK_SIZE:
                self._receive_blocksize_info(length)
            else:
                data = self._receive(length)
                log.warning("Dropping unknown info reply=%r data=%r",
                            info, data)

    def _format_go_option_data(self, export_name, *requests):
        """
        Format export name and optional list of NBD_INFO_XXX requests.

        32 bits, length of name (unsigned); MUST be no larger than the option
            data length - 6
        String: name of the export
        16 bits, number of information requests
        16 bits x n - list of NBD_INFO information requests
        """
        # Export name (length + name)
        name = export_name.encode("utf-8")
        data = bytearray()
        data += struct.pack("!I", len(name))
        data += name

        # Information requests list (length + requests)
        data += struct.pack("!H", len(requests))
        if requests:
            data += struct.pack("!%dH" % len(requests), *requests)

        return data

    def _receive_export_info(self, length):
        if length != 10:
            raise Error("Invalid export info length {}"
                        .format(length))
        self.export_size, self.transmission_flags = self._receive_struct("!QH")
        log.debug("Received export info [size=%r flags=%r]",
                  self.export_size, self.transmission_flags)

    def _receive_blocksize_info(self, length):
        if length != 12:
            raise Error("Invalid blocksize info length {}"
                        .format(length))
        (self.minimum_block_size, self.preferred_block_size,
            self.maximum_block_size) = self._receive_struct("!III")
        log.debug("Received block size info [minimum=%r preferred=%r "
                  "maximum=%r]",
                  self.minimum_block_size,
                  self.preferred_block_size,
                  self.maximum_block_size)

    # Negotiating options

    def _negotiate_option(self, opt, data=b""):
        self._send_option(opt, data)
        reply, length = self._receive_option_reply(opt)

        if reply in ERROR_REPLY:
            self._handle_option_error(opt, reply, length)

        # The spec is not clear about the possible reply for general options.
        # using qemu policy as in nbd_request_simple_option().

        if reply != NBD_REP_ACK:
            raise Error("Unexpected reply {} for option {}".format(reply, opt))

        if length != 0:
            raise Error("Reply with non-zero length {} for option {}"
                        .format(length, opt))

    def _send_option(self, opt, data=b""):
        """
        Send an option with optional data to the server. The caller must call
        _receive_option_reply() to get a reply.

        C: 64 bits, 0x49484156454F5054 (ASCII 'IHAVEOPT')
        C: 32 bits, option
        C: 32 bits, length of option data (unsigned)
        C: any data needed for the chosen option, of length as specified above.
        """
        log.debug("Sending option: %r data: %r", opt, data)
        self._send_struct("!QII", IHAVEOPT, opt, len(data))
        if data:
            self._send(data)

    def _receive_option_reply(self, expected_option):
        """
        Receive reply header from server, and return the reply and the length
        of the data that the caller need to read from the server to complete
        the option negotiation.

        S: 64 bits, 0x3e889045565a9 (magic number for replies)
        S: 32 bits, the option as sent by the client to which this is a reply
        S: 32 bits, reply type:
            - NBD_REP_ACK for successful completion, or
            - NBD_REP_ERR_UNSUP option not known by this server
        S: 32 bits, length of the reply; if zero, next field is not sent
        S: any data as required by the reply.
        """
        magic, option, reply, length = self._receive_struct("!QIII")
        log.debug("Received reply [magic=%x option=%s type=%s len=%s]",
                  magic, option, reply, length)

        if magic != OPTION_REPLY_MAGIC:
            raise Error("Unexpected reply magic number {:x}, expecting {:x}"
                        .format(magic, OPTION_REPLY_MAGIC))

        if option != expected_option:
            raise Error("Unexpected reply option {:x}, expecting {:x}"
                        .format(option, expected_option))

        return reply, length

    def _handle_option_error(self, opt, reply, length):
        """
        Consume the optional data which is an error message suitable for
        displaying to the user, and raise an OptionError.
        """
        message = ""

        # If the server sent an error message, try to use it.
        if length:
            message = self._receive(length).decode("utf-8", errors="replace")

        # If we have no message, use the builtin message for this error.
        if message == "":
            message = strerror(reply)

        if reply == NBD_REP_ERR_UNSUP:
            raise OptionUnsupported(opt, message)
        else:
            raise OptionError(opt, reply, message)

    # Terminating session

    def _soft_disconnect(self):
        """
        Perform soft disconnect.

        During handshake, we need to send a NBD_OPT_ABORT. The server
        may reply, but we are allowed to close the socket without
        reading the reply[1].

        During transmission, we need to send a NBD_CMD_DISC. The
        server does not reply[2].

        [1] https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
            #termination-of-the-session-during-option-haggling
        [2] https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
            #terminating-the-transmission-phase
        """
        log.info("Initiating a soft disconnect")
        try:
            if self._state == HANDSHAKE:
                self._send_option(NBD_OPT_ABORT)
            elif self._state == TRASMISSION:
                handle = next(self._counter)
                self._send_command(NBD_CMD_DISC, handle, 0, 0)
            else:
                raise AssertionError(
                    "Cannot initiate soft disconnect at state {!r}"
                    .foramt(self._state))
        except socket.error as e:
            log.debug("Error initiating soft disconnect: %s", e)
        except Exception:
            log.exception("Error initiating soft disconnect")

        self._state = CLOSED
        self._close_socket()

    def _hard_disconnect(self):
        if self._state < CLOSED:
            log.info("Initiating a hard disconnect")
            self._state = CLOSED
            self._close_socket()

    def _close_socket(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            log.debug("Error shutting down socket: %s", e)
        self._sock.close()

    # Commands

    def _send_command(self, type, handle, offset, length):
        # C: 32 bits, 0x25609513, magic (NBD_REQUEST_MAGIC)
        # C: 16 bits, command flags
        # C: 16 bits, type
        # C: 64 bits, handle
        # C: 64 bits, offset (unsigned)
        # C: 32 bits, length (unsigned)
        # C: (length bytes of data if the request is of type NBD_CMD_WRITE)
        log.debug("Sending command type=%s handle=%s offset=%s length=%s",
                  type, handle, offset, length)
        self._send_struct("!IHHQQI", NBD_REQUEST_MAGIC, 0, type, handle,
                          offset, length)

    def _receive_simple_reply(self, expected_handle):
        # Simple reply
        # S: 32 bits, 0x67446698, magic (NBD_SIMPLE_REPLY_MAGIC)
        # S: 32 bits, error (MAY be zero)
        # S: 64 bits, handle
        # S: (length bytes of data if the request is of type NBD_CMD_READ and
        #    error is zero)
        magic, error, handle = self._receive_struct("!IIQ")

        if magic != NBD_SIMPLE_REPLY_MAGIC:
            raise Error("Unexpected reply magic {!r}, expecting {!r}"
                        .format(magic, NBD_SIMPLE_REPLY_MAGIC))

        if error != 0:
            raise Error("Error {}: {}".format(error, strerror(error)))

        if handle != expected_handle:
            raise Error("Unepected handle {}, expecting {}"
                        .format(handle, expected_handle))

    # Structured I/O

    def _receive_struct(self, fmt):
        s = struct.Struct(fmt)
        data = self._receive(s.size)
        return s.unpack(data)

    def _send_struct(self, fmt, *args):
        data = struct.pack(fmt, *args)
        self._sock.sendall(data)

    # Plain I/O

    def _send(self, data):
        self._sock.sendall(data)

    def _receive(self, length):
        buf = bytearray(length)
        self._receive_into(buf)
        return buf

    def _receive_into(self, buf):
        length = len(buf)
        pos = 0
        while pos < length:
            view = memoryview(buf)[pos:]
            n = util.uninterruptible(self._sock.recv_into, view)
            if not n:
                raise Error("Server closed the connection, read {} bytes, "
                            "expected {} bytes"
                            .format(pos, length))
            pos += n

    # Conetext manager

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        try:
            self.close()
        except Exception:
            # Don't hide excpetions in user code.
            if t is None:
                raise
            log.exeption("Error closing")


def strerror(error):
    return ERROR_REPLY.get(error, "Unknown error")
