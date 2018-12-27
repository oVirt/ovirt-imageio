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

from . import util

# Supported urls:
# - nbd:unix:/sock
# - nbd:unix:/sock:exportname=
# - nbd:unix:/sock:exportname=sda
UNIX_URL = re.compile(r"unix:(?P<sock>/[^:]+)(:?:exportname=(?P<name>.*))?")

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


def open(url):
    """
    Parse nbd url and return a connected client.

    Currnetly only nbd:unix:/path:exportname=foo is supported.
    """
    if url.scheme != "nbd":
        raise Error("Unsupported URL scheme %s" % url)

    m = UNIX_URL.match(url.path)
    if m is None:
        raise Error("Unsupported URL path %r" % url)

    d = m.groupdict()
    return Client(d["sock"], export_name=d["name"] or "")


class Client(object):

    def __init__(self, socket_path, export_name=""):
        self.export_size = None
        self.transmission_flags = None
        self.minimum_block_size = None
        self.preferred_block_size = None
        self.maximum_block_size = None
        self._counter = itertools.count()

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            log.info("Connecting to %r %r", socket_path, export_name)
            self._sock.connect(socket_path)
            self._newstyle_handshake(export_name)
            log.info("Ready for transmission")
        except:
            self.close()
            raise

    def read(self, offset, length):
        handle = next(self._counter)
        self._send_command(NBD_CMD_READ, handle, offset, length)
        self._receive_simple_reply(handle)
        return self._receive(length)

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
        self._sock.close()

    # NBD fixed newstyle handshake

    def _newstyle_handshake(self, export_name):
        # Initial handshake.
        nbd_magic, cliserv_magic, server_flags = self._receive_struct("!QQH")

        if nbd_magic != NBDMAGIC:
            raise Error("Bad nbd magic {:x}, expecting {:x}"
                        .format(nbd_magic, NBDMAGIC))

        if cliserv_magic != IHAVEOPT:
            raise Error("Server does not support newsyle negotiation "
                        "[magic={:x} expected={:x}]"
                        .format(cliserv_magic, IHAVEOPT))

        log.debug("Received server flags: %x", server_flags)
        if not server_flags & NBD_FLAG_FIXED_NEWSTYLE:
            raise Error("Server does not support fixed newstyle negotiation")

        self._send_client_flags(NBD_FLAG_C_FIXED_NEWSTYLE)

        # Options haggling.
        self._send_go_option(export_name)
        self._receive_go_reply()

    def _send_client_flags(self, flags):
        log.debug("Sending client flags: %x:", flags)
        self._send_struct("!I", flags)

    # Options

    def _send_go_option(self, export_name):
        name = export_name.encode("utf-8")
        data = bytearray()
        data += struct.pack("!I", len(name))
        data += name
        data += struct.pack("!H", 0)
        head = struct.pack("!QII", IHAVEOPT, NBD_OPT_GO, len(data))
        log.debug("Sending option: %r data: %r", head, data)
        self._send(head + data)

    def _receive_go_reply(self):
        while True:
            reply, length = self._receive_option_reply(NBD_OPT_GO)

            if reply in ERROR_REPLY:
                message = self._receive_error_reply(length)
                raise Error("Error {}: {} ({})"
                            .format(reply, strerror(reply), message))

            if reply == NBD_REP_ACK:
                if self.export_size is None or self.transmission_flags is None:
                    raise Error("Server did not send export info")
                break

            if reply != NBD_REP_INFO:
                raise Error("Unexpected reply {:x}, expecting info reply {:x}"
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
                log.debug("Dropping unknown info reply %r", info)

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

    def _receive_option_reply(self, expected_option):
        magic, option, reply, length = self._receive_struct("!QIII")
        log.debug("Received reply [magic=%x option=%x type=%x len=%r]",
                  magic, option, reply, length)

        if magic != OPTION_REPLY_MAGIC:
            raise Error("Unexpected reply magic number {:x}, expecting {:x}"
                        .format(magic, OPTION_REPLY_MAGIC))

        if option != expected_option:
            raise Error("Unexpected reply option {:x}, expecting {:x}"
                        .format(option, expected_option))

        return reply, length

    def _receive_error_reply(self, length):
        if not length:
            return ""
        error = self._receive(length)
        try:
            return error.decode("utf-8")
        except UnicodeDecodeError:
            return "(error decoding error message)"

    # Commands

    def _send_command(self, type, handle, offset, legnth):
        # C: 32 bits, 0x25609513, magic (NBD_REQUEST_MAGIC)
        # C: 16 bits, command flags
        # C: 16 bits, type
        # C: 64 bits, handle
        # C: 64 bits, offset (unsigned)
        # C: 32 bits, length (unsigned)
        # C: (length bytes of data if the request is of type NBD_CMD_WRITE)
        self._send_struct("!IHHQQI", NBD_REQUEST_MAGIC, 0, type, handle,
                          offset, legnth)

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
        data = bytearray(length)
        pos = 0
        while pos < length:
            buf = memoryview(data)[pos:]
            n = util.uninterruptible(self._sock.recv_into, buf)
            if not n:
                raise Error("Server closed the connection, read {} bytes, "
                            "expected {} bytes"
                            .format(pos, length))
            pos += n
        return data

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
