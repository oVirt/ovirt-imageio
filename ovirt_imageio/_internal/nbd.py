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

import errno
import itertools
import logging
import os
import re
import socket
import struct

from . import ipv6
from . import sockutil

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
REQUEST_MAGIC = 0x25609513
SIMPLE_REPLY_MAGIC = 0x67446698
STRUCTURED_REPLY_MAGIC = 0x668e33ef

# Flags
FLAG_FIXED_NEWSTYLE = 1
FLAG_NO_ZEROES = 2
FLAG_C_FIXED_NEWSTYLE = 1
FLAG_C_NO_ZEROES = 2

# Transmission flags
FLAG_HAS_FLAGS = (1 << 0)
FLAG_READ_ONLY = (1 << 1)
FLAG_SEND_FLUSH = (1 << 2)
FLAG_SEND_FUA = (1 << 3)
FLAG_ROTATIONAL = (1 << 4)
FLAG_SEND_TRIM = (1 << 5)
FLAG_SEND_WRITE_ZEROES = (1 << 6)
FLAG_SEND_DF = (1 << 7)
FLAG_CAN_MULTI_CONN = (1 << 8)
FLAG_SEND_RESIZE = (1 << 9)
FLAG_SEND_CACHE = (1 << 10)

# Options
OPT_ABORT = 2
OPT_GO = 7
OPT_STRUCTURED_REPLY = 8
OPT_LIST_META_CONTEXT = 9
OPT_SET_META_CONTEXT = 10

# Replies
REP_ACK = 1
REP_INFO = 3
REP_META_CONTEXT = 4

# Structured reply flags
REPLY_FLAG_DONE = (1 << 0)

# Flags for base:allocation meta context.

# This range does not allocate any data on storage. Examples are a hole
# in raw image, or zero cluster in qcow2 image. This is flag is optional
# and may be ommited by a NBD server. Cannot be used to detect
# unallocated areas in qcow2 exposing data from the backing file.
STATE_HOLE = (1 << 0)

# The range is read as zero.
STATE_ZERO = (1 << 1)

# Flags for qemu:dirty-bitmap meta context.
STATE_DIRTY = (1 << 0)

# Adusted to support merging allocation and dirty bits.
EXTENT_DIRTY = (1 << 2)

# Extent does not exist, exposing data from the backing file.
EXTENT_BACKING = (1 << 3)

# Command flags
CMD_FLAG_NO_HOLE = (1 << 1)

# Structured reply types
REPLY_TYPE_NONE = 0
REPLY_TYPE_OFFSET_DATA = 1
REPLY_TYPE_OFFSET_HOLE = 2
REPLY_TYPE_BLOCK_STATUS = 5
REPLY_ERROR_BASE = (1 << 15)
REPLY_TYPE_ERROR = REPLY_ERROR_BASE + 1
REPLY_TYPE_ERROR_OFFSET = REPLY_ERROR_BASE + 2

# NBD_INFO replies
INFO_EXPORT = 0
INFO_BLOCK_SIZE = 3

# Meta context names.
BASE_ALLOCATION = "base:allocation"
QEMU_ALLOCATION_DEPTH = "qemu:allocation-depth"
QEMU_DIRTY_BITMAP = "qemu:dirty-bitmap:"

# Error replies
ERR_BASE = 2**31
REP_ERR_UNSUP = ERR_BASE + 1
REP_ERR_POLICY = ERR_BASE + 2
REP_ERR_INVALID = ERR_BASE + 3
REP_ERR_PLATFORM = ERR_BASE + 4
REP_ERR_TLS_REQD = ERR_BASE + 5
REP_ERR_UNKNOWN = ERR_BASE + 6
REP_ERR_SHUTDOWN = ERR_BASE + 7
REP_ERR_BLOCK_SIZE_REQD = ERR_BASE + 8
REP_ERR_TOO_BIG = ERR_BASE + 9

ERROR_REPLY = {
    REP_ERR_UNSUP: (
        "The option sent by the client is unknown by this server "
        "implementation"),
    REP_ERR_POLICY: (
        "The option sent by the client is known by this server and "
        "syntactically valid, but server-side policy forbids the server to "
        "allow the option"),
    REP_ERR_INVALID: (
        "The option sent by the client is known by this server, but was "
        "determined by the server to be syntactically or semantically "
        "invalid"),
    REP_ERR_PLATFORM: (
        "The option sent by the client is not supported on the platform on "
        "which the server is running"),
    REP_ERR_TLS_REQD: (
        "The server is unwilling to continue negotiation unless TLS is "
        "initiated first"),
    REP_ERR_UNKNOWN: "The requested export is not available",
    REP_ERR_SHUTDOWN: (
        "The server is unwilling to continue negotiation as it is in the "
        "process of being shut down"),
    REP_ERR_BLOCK_SIZE_REQD: (
        "The server is unwilling to enter transmission phase for a given "
        "export unless the client first acknowledges (via "
        "INFO_BLOCK_SIZE) that it will obey non-default block sizing "
        "requirements"),
    REP_ERR_TOO_BIG: "The request or the reply is too large to process",
}

# Mapping from NBD error code in simple or structured reply to system errno.
# https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
# #error-values
REPLY_ERRORS = {
    1: errno.EPERM,
    5: errno.EIO,
    12: errno.ENOMEM,
    22: errno.EINVAL,
    28: errno.ENOSPC,
    75: errno.EOVERFLOW,
    108: errno.ESHUTDOWN,
}

# NBD Option struct
# C: 64 bits, 0x49484156454F5054 (ASCII 'IHAVEOPT')
# C: 32 bits, option
# C: 32 bits, length of option data (unsigned)
OPTION = struct.Struct("!QII")

# Maximum NBD request length (unsigned 32 bit integer).
MAX_LENGTH = 2**32 - 1

# The NBD spec does not define how many extents chunks a server may send in
# REPLY_TYPE_BLOCK_STATUS.  Theoretically a server can retrun one extent per
# byte if the minimum block size is 1 byte. Practically for raw images minimum
# extent size is the file system block size (likely 4 KiB), and for qcow2
# images the cluster size (likely 64 KiB). Since we don't know the image
# format, assume raw image. If a server send more extents than this value we
# fail the connection.
MAX_EXTENTS = MAX_LENGTH // 4096

log = logging.getLogger("nbd")


class Error(Exception):
    fmt = "{self.reason}"

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.fmt.format(self=self)


class ProtocolError(Error):
    """
    Raised when server sent invalid response. Requires termination of the
    connection.
    """


class UnexpectedOptionReply(ProtocolError):
    fmt = ("Unexpected reply {self.reply} for option {self.option}, "
           "expecting reply {self.expected}")

    def __init__(self, reply, option, expected):
        self.reply = reply
        self.option = option
        self.expected = expected


class InvalidLength(ProtocolError):
    fmt = ("Reply {self.reply} with invalid legnth {self.length}, expecting "
           "{self.expected}")

    def __init__(self, reply, length, expected):
        self.reply = reply
        self.length = length
        self.expected = expected


class UnexpectedHandle(ProtocolError):
    fmt = "Unepected handle {self.handle}, expecting {self.expected}"

    def __init__(self, handle, expected):
        self.handle = handle
        self.expected = expected


class OptionError(Error):
    fmt = ("Error negotiating option opt={self.opt} code={self.code} "
           "reason={self.reason}")

    def __init__(self, opt, code, reason):
        self.opt = opt
        self.code = code
        self.reason = reason


class OptionUnsupported(OptionError):
    fmt = "Option {self.option} is not supported: {self.reason}"
    code = REP_ERR_UNSUP

    def __init__(self, option, reason):
        self.option = option
        self.reason = reason


class RequestError(Error):
    """
    Raised when server failed to process a request. The client can continue
    normally with another request or repeat the failing request.
    """


class UnsupportedRequest(RequestError):
    """
    Raised when the server cannot process a request becuase the requested
    operation is not supported for the current connection. The client should
    not send the same request again.
    """


class ReplyError(RequestError):
    """
    Raised when server return an error reply.
    """
    fmt = "{self.message}: [Error {self.code}] {self.reason}"

    def __init__(self, code, message):
        """
        Arguments:
            code (int): NBD reply error code.
            message (str): string suitable for displaying to the user.
        """
        # Message is optional, but qemu-nbd always sends a message.
        if not message:
            message = "Server error"

        self.code = code
        self.message = message.capitalize()
        self.reason = os.strerror(REPLY_ERRORS.get(code, code))


class UnixAddress(sockutil.UnixAddress):
    """
    sockutil.UnixAddress enriched with url() providing nbd URL.
    """

    def url(self, export=None):
        s = "nbd:unix:{}".format(self.path)
        if export:
            s += ":exportname=" + export
        return s


class TCPAddress(sockutil.TCPAddress):
    """
    sockutil.TCPAddress enriched with url() providing nbd URL.
    """
    def url(self, export=None):
        host = ipv6.quote_address(self.host)
        s = "nbd:{}:{}".format(host, self.port)
        if export:
            s += ":exportname=" + export
        return s


def open(url, dirty=False):
    """
    Open parsed NBD URL and return a connected Client instance.
    """
    address, name = _parse_url(url)
    return Client(address, name, dirty=dirty)


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
        export = url.path
        # According to NBD spec (and qemu implementation) the / starting the
        # path component of the URL is not considered part of the export name.
        # To create export name with leading /, the path must start with //.
        # https://github.com/NetworkBlockDevice/nbd/blob/master/doc/uri.md
        if export.startswith("/"):
            export = export[1:]
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
TRANSMISSION = 2
CLOSED = 3


class Client:

    def __init__(self, address, export_name=None, dirty=False):
        self.address = address
        self.export_name = export_name or ""
        self.dirty = dirty

        log.debug("Connecting address=%r export_name=%r dirty=%r",
                  address, self.export_name, dirty)

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

        # Set to "qemu:dirty-bitmap:bitmap-name" if dirty is True, server
        # supports structued replies, and exports a dirty bitmap. Use this name
        # with extents() results to extract dirty extents.
        self.dirty_bitmap = None

        # Server capabilities discovered during handshake.
        self._structured_reply = False
        self._meta_context = {}

        self._counter = itertools.count()
        self._state = CONNECTING

        self._sock = self._connect(address)
        try:
            self._newstyle_handshake(dirty)
        except:  # noqa: E722
            self.close()
            raise

        log.debug("Ready for transmission")

    @property
    def has_base_allocation(self):
        return BASE_ALLOCATION in self._meta_context

    @property
    def has_allocation_depth(self):
        return QEMU_ALLOCATION_DEPTH in self._meta_context

    def read(self, offset, length):
        buf = bytearray(length)
        self.readinto(offset, buf)
        return buf

    def readinto(self, offset, buf):
        # If structured reply was negotiated, the server must send structured
        # reply to NBD_CMD_READ.
        cmd = Read(
            self._next_handle(), offset, buf,
            only_structured=self._structured_reply)
        self._send_command(cmd)
        self._recv_reply(cmd)
        return len(buf)

    def write(self, offset, data):
        cmd = Write(self._next_handle(), offset, len(data))
        self._send_command(cmd)
        self._send(data)
        self._recv_reply(cmd)

    def zero(self, offset, length, punch_hole=True):
        if self.transmission_flags & FLAG_SEND_WRITE_ZEROES == 0:
            raise UnsupportedRequest(
                "Server does not support CMD_WRITE_ZEROES")
        flags = 0 if punch_hole else CMD_FLAG_NO_HOLE
        cmd = WriteZeroes(self._next_handle(), offset, length, flags=flags)
        self._send_command(cmd)
        self._recv_reply(cmd)

    def flush(self):
        # TODO: is this the best way to handle this?
        if self.transmission_flags & FLAG_SEND_FLUSH == 0:
            return
        cmd = Flush(self._next_handle())
        self._send_command(cmd)
        self._recv_reply(cmd)

    def extents(self, offset, length):
        cmd = BlockStatus(self._next_handle(), offset, length)
        self._send_command(cmd)
        self._recv_reply(cmd)
        return cmd.reply

    def close(self):
        if self._state in (HANDSHAKE, TRANSMISSION):
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
        except:  # noqa: E722
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
        except:  # noqa: E722
            sock.close()
            raise

        return sock

    # NBD fixed newstyle handshake

    def _newstyle_handshake(self, dirty=False):
        assert self._state == CONNECTING
        self._state = HANDSHAKE

        # Initial handshake.
        nbd_magic, cliserv_magic, server_flags = self._recv_fmt("!QQH")

        if nbd_magic != NBDMAGIC:
            raise ProtocolError("Bad nbd magic {:x}, expecting {:x}"
                                .format(nbd_magic, NBDMAGIC))

        if cliserv_magic != IHAVEOPT:
            raise ProtocolError("Server does not support newstyle negotiation "
                                "magic={:x} expected={:x}"
                                .format(cliserv_magic, IHAVEOPT))

        log.debug("Received server flags: %x", server_flags)
        if not server_flags & FLAG_FIXED_NEWSTYLE:
            raise ProtocolError(
                "Server does not support fixed newstyle negotiation")

        self._send_client_flags(FLAG_C_FIXED_NEWSTYLE)

        # Options haggling.

        self._negotiate_structured_reply_option()

        if self._structured_reply:
            dirty_bitmap = self._query_dirty_bitmap() if dirty else None
            self._set_meta_context(dirty_bitmap)

        self._negotiate_go_option()

        self._state = TRANSMISSION

    def _send_client_flags(self, flags):
        log.debug("Sending client flags: %x", flags)
        b = struct.pack("!I", flags)
        self._send(b)

    # Negotiating options

    def _negotiate_structured_reply_option(self):
        """
        Ask the server to enable structured replies. This allows better error
        handling for CMD_READ, and enables extension that require
        structured replies such as CMD_BLOCK_STATUS.

        If negotiation was successful, the server MUST use structured reply to
        any response with a payload, and may used structured reply for other
        responses.

        If the server fails with REP_ERR_UNSUP, we disable structured
        replies and will not be able to report block status.
        """
        try:
            self._negotiate_option(OPT_STRUCTURED_REPLY)
        except OptionUnsupported as e:
            log.warning("Structured reply is not available: %s", e)
        else:
            log.debug("Structured reply enabled")
            self._structured_reply = True

    def _query_dirty_bitmap(self):
        """
        Query the server for dirty bitmap and return the context name if the
        server exports one, or None if server does not export any, or exports
        more than one.
        """
        opt = OPT_LIST_META_CONTEXT
        data = self._format_meta_context_data(QEMU_DIRTY_BITMAP)
        self._send_option(opt, data)

        bitmaps = list(self._iter_meta_context_replies(opt))

        if len(bitmaps) == 0:
            log.warning(
                "Server does not support %s meta context", QEMU_DIRTY_BITMAP)
            return None

        if len(bitmaps) > 1:
            log.warning("Cannot use multiple dirty bitmaps: %s", bitmaps)
            return None

        ctx_name, _ = bitmaps[0]
        log.debug("Server has dirty bitmap %s", ctx_name)
        return ctx_name

    def _set_meta_context(self, dirty_bitmap=None):
        """
        Register wanted meta context with the server.
        """
        opt = OPT_SET_META_CONTEXT

        # qemu:allocation-depth is required to detect holes in qcow2 images -
        # unallocated clusters exposing data from the backing chain.
        # Added in qemu 5.2.0.
        queries = [BASE_ALLOCATION, QEMU_ALLOCATION_DEPTH]
        if dirty_bitmap:
            queries.append(dirty_bitmap)

        data = self._format_meta_context_data(*queries)
        self._send_option(opt, data)

        # If the server supports OPT_SET_META_CONTEXT and all the contexts
        # in self._meta_context, we expect to get one reply for every context,
        # with the meta context id, and then REP_ACK.
        #
        # If the server does not support meta context, we may get
        # REP_ERR_UNSUP. If the server supports OPT_SET_META_CONTEXT
        # but not all required meta contexts, we may get info only about the
        # contexts supported by the server.
        #
        # Related sections in the spec:
        # - https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
        #   #metadata-querying
        # - https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
        #   #option-types (see OPT_SET_META_CONTEXT).

        for ctx_name, ctx_id in self._iter_meta_context_replies(opt):
            if ctx_name not in queries:
                raise ProtocolError(
                    "Unexpected context {}, expecting one of {}"
                    .format(ctx_name, queries))

            log.debug("Meta context %s is available id=%s", ctx_name, ctx_id)

            # Keep also reverse mapping to find name from id.
            self._meta_context[ctx_name] = ctx_id
            self._meta_context[ctx_id] = ctx_name

            if ctx_name == dirty_bitmap:
                self.dirty_bitmap = dirty_bitmap

        # Log if some meta context is not available. This may affect
        # performance or reduce funcionaliity, but is expected when using old
        # qemu-nbd (e.g. 4.2.0) or when using raw volume that does not provide
        # interesting allocation depth, and triggers a bug in qemu 6.2.0.
        for ctx_name in queries:
            if ctx_name not in self._meta_context:
                log.info("Meta context %s is not available", ctx_name)

    def _format_meta_context_data(self, *queries):
        """
        32 bits, length of export name.
        String, name of export for which we wish to list metadata contexts.
        32 bits, number of queries
        Zero or more queries, each being:
            32 bits, length of query
            String, query to select metadata contexts.
        """
        # Export name (length + name)
        name = self.export_name.encode("utf-8")
        data = bytearray()
        data += struct.pack("!I", len(name))
        data += name

        # Queries
        data += struct.pack("!I", len(queries))
        for query in queries:
            query = query.encode("utf-8")
            data += struct.pack("!I", len(query))
            data += query

        return data

    def _iter_meta_context_replies(self, opt):
        """
        Receive replies for NBD_LIST_META_CONTEXT and NBD_OPT_SET_META_CONTEXT
        commands. We assume that the replies and errors are identical.
        """
        while True:
            reply, length = self._recv_option_reply(opt)

            if reply in ERROR_REPLY:
                try:
                    self._handle_option_error(opt, reply, length)
                except OptionUnsupported as e:
                    log.warning("Meta context is not supported: %s", e)
                    return

            if reply == REP_ACK:
                if length != 0:
                    raise InvalidLength(reply, length, 0)
                break

            if reply != REP_META_CONTEXT:
                raise UnexpectedOptionReply(reply, opt, REP_META_CONTEXT)

            yield self._recv_meta_context_reply(length)

    def _recv_meta_context_reply(self, length):
        """
        Receive reply to OPT_SET_META_CONTEXT, and store the meta context
        id in self._meta_context dict.

        32 bits, NBD metadata context ID.
        String, name of the metadata context. This is not required to be a
            human-readable string, but it MUST be valid UTF-8 data.
        """
        if length < 4:
            raise InvalidLength(REP_META_CONTEXT, length, ">= 4")

        data = self._recv(length)
        ctx_id = struct.unpack("!I", data[:4])[0]
        ctx_name = data[4:].decode("utf-8")

        return ctx_name, ctx_id

    def _negotiate_go_option(self):
        # Here we can announce that we can honour server block size constraints
        # by adding INFO_BLOCK_SIZE information request. If we do this we
        # MUST abide by the block size constraints received. If we don't we are
        # allowed to send unaligned requests.
        # https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
        # section #block-size-constraints

        opt = OPT_GO
        data = self._format_go_option_data()
        self._send_option(opt, data)

        while True:
            reply, length = self._recv_option_reply(opt)

            if reply in ERROR_REPLY:
                self._handle_option_error(opt, reply, length)

            if reply == REP_ACK:
                if length != 0:
                    raise InvalidLength(reply, length, 0)
                if self.export_size is None or self.transmission_flags is None:
                    raise ProtocolError("Server did not send export size or "
                                        "transmission flags")
                break

            if reply != REP_INFO:
                raise UnexpectedOptionReply(reply, opt, REP_INFO)

            if length < 2:
                raise InvalidLength(REP_INFO, length, ">= 2")

            info = self._recv_fmt("!H")[0]
            length -= 2

            if info == INFO_EXPORT:
                self._recv_export_info(length)
            elif info == INFO_BLOCK_SIZE:
                self._recv_blocksize_info(length)
            else:
                data = self._recv(length)
                log.warning("Dropping unknown info reply=%r data=%r",
                            info, data)

    def _format_go_option_data(self, *requests):
        """
        Format export name and optional list of INFO_XXX requests.

        32 bits, length of name (unsigned); MUST be no larger than the option
            data length - 6
        String: name of the export
        16 bits, number of information requests
        16 bits x n - list of INFO information requests
        """
        # Export name (length + name)
        name = self.export_name.encode("utf-8")
        data = bytearray()
        data += struct.pack("!I", len(name))
        data += name

        # Information requests list (length + requests)
        data += struct.pack("!H", len(requests))
        if requests:
            data += struct.pack("!%dH" % len(requests), *requests)

        return data

    def _recv_export_info(self, length):
        if length != 10:
            raise InvalidLength(REP_INFO, length, 10)
        self.export_size, self.transmission_flags = self._recv_fmt("!QH")
        log.debug("Received export info size=%r flags=%r",
                  self.export_size, self.transmission_flags)

    def _recv_blocksize_info(self, length):
        if length != 12:
            raise InvalidLength(REP_INFO, length, 12)
        (self.minimum_block_size, self.preferred_block_size,
            self.maximum_block_size) = self._recv_fmt("!III")
        log.debug("Received block size info minimum=%r preferred=%r "
                  "maximum=%r",
                  self.minimum_block_size,
                  self.preferred_block_size,
                  self.maximum_block_size)

    # Negotiating options

    def _negotiate_option(self, opt, data=b""):
        self._send_option(opt, data)
        reply, length = self._recv_option_reply(opt)

        if reply in ERROR_REPLY:
            self._handle_option_error(opt, reply, length)

        # The spec is not clear about the possible reply for general options.
        # using qemu policy as in nbd_request_simple_option().

        if reply != REP_ACK:
            raise UnexpectedOptionReply(reply, opt, REP_ACK)

        if length != 0:
            raise InvalidLength(reply, length, 0)

    def _send_option(self, opt, data=b""):
        """
        Send an option with optional data to the server. The caller must call
        _recv_option_reply() to get a reply.
        """
        log.debug("Sending option: %r data: %r", opt, data)
        b = OPTION.pack(IHAVEOPT, opt, len(data))
        self._send(b)
        if data:
            self._send(data)

    def _recv_option_reply(self, expected_option):
        """
        Receive reply header from server, and return the reply and the length
        of the data that the caller need to read from the server to complete
        the option negotiation.

        S: 64 bits, 0x3e889045565a9 (magic number for replies)
        S: 32 bits, the option as sent by the client to which this is a reply
        S: 32 bits, reply type:
            - REP_ACK for successful completion, or
            - REP_ERR_UNSUP option not known by this server
        S: 32 bits, length of the reply; if zero, next field is not sent
        S: any data as required by the reply.
        """
        magic, option, reply, length = self._recv_fmt("!QIII")
        log.debug("Received reply magic=%x option=%s type=%s len=%s",
                  magic, option, reply, length)

        if magic != OPTION_REPLY_MAGIC:
            raise ProtocolError(
                "Unexpected reply magic {:x} for option {}, expecting {:x}"
                .format(magic, expected_option, OPTION_REPLY_MAGIC))

        if option != expected_option:
            raise ProtocolError("Unexpected reply option {}, expecting {}"
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
            message = self._recv(length).decode("utf-8", errors="replace")

        # If we have no message, use the builtin message for this error.
        if message == "":
            message = ERROR_REPLY.get(reply, "Unknown error")

        if reply == REP_ERR_UNSUP:
            raise OptionUnsupported(opt, message)
        else:
            raise OptionError(opt, reply, message)

    # Terminating session

    def _soft_disconnect(self):
        """
        Perform soft disconnect.

        During handshake, we need to send a OPT_ABORT. The server
        may reply, but we are allowed to close the socket without
        reading the reply[1].

        During transmission, we need to send a CMD_DISC. The
        server does not reply[2].

        [1] https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
            #termination-of-the-session-during-option-haggling
        [2] https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
            #terminating-the-transmission-phase
        """
        log.debug("Initiating a soft disconnect")
        try:
            if self._state == HANDSHAKE:
                self._send_option(OPT_ABORT)
            elif self._state == TRANSMISSION:
                cmd = Disc(self._next_handle())
                self._send_command(cmd)
            else:
                raise AssertionError(
                    "Cannot initiate soft disconnect at state {!r}"
                    .format(self._state))
        except socket.error as e:
            log.debug("Error initiating soft disconnect: %s", e)
        except Exception:
            log.exception("Error initiating soft disconnect")

        self._state = CLOSED
        self._close_socket()

    def _hard_disconnect(self):
        if self._state < CLOSED:
            log.debug("Initiating a hard disconnect")
            self._state = CLOSED
            self._close_socket()

    def _close_socket(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            log.debug("Error shutting down socket: %s", e)
        self._sock.close()

    # Commands

    def _next_handle(self):
        return next(self._counter)

    def _send_command(self, cmd):
        log.debug("Sending %s", cmd)
        self._send(cmd.to_bytes())

    def _recv_reply(self, cmd):
        """
        Receive either a simple reply or structured reply for cmd.
        """
        while True:
            magic = self._recv_fmt("!I")[0]

            if magic == SIMPLE_REPLY_MAGIC:
                if cmd.only_structured:
                    raise ProtocolError(
                        "Unexpected simple reply magic {:x}, expecting "
                        "structured reply magic {:x}"
                        .format(magic, STRUCTURED_REPLY_MAGIC))

                self._recv_simple_reply(cmd)
                return

            elif magic == STRUCTURED_REPLY_MAGIC:
                if not self._structured_reply:
                    raise ProtocolError(
                        "Unexpected structured reply magic {:x}, expecting "
                        "simple reply magic {:x}"
                        .format(magic, SIMPLE_REPLY_MAGIC))

                # We started to received structured reply chunks, so simple
                # reply is not allowed.
                cmd.only_structured = True

                if self._recv_reply_chunk(cmd):
                    break

            else:
                raise ProtocolError("Unexpected reply magic {:x}"
                                    .format(magic))

        if cmd.errors:
            # Some chunks failed. We don't have a good way to report
            # partial failures since content chunks may be fragmented, so
            # fail the entire request.
            raise RequestError("Errors receiving reply: {}".format(cmd.errors))

    def _recv_simple_reply(self, cmd):
        """
        Receive a simple reply (magic was already read).

        S: 32 bits, error (MAY be zero)
        S: 64 bits, handle
        S: (length bytes of data if the request is of type CMD_READ and
           error is zero)
        """
        error, handle = self._recv_fmt("!IQ")

        if error != 0:
            # We have no context in this case.
            raise ReplyError(error, "Simple reply failed")

        if handle != cmd.handle:
            raise UnexpectedHandle(handle, cmd.handle)

        if cmd.buf:
            self._recv_into(cmd.buf)

    def _recv_reply_chunk(self, cmd):
        """
        Receive a structured reply chunk (magic was already read). Return True
        if this was the last chunk.

        S: 16 bits, flags
        S: 16 bits, type
        S: 64 bits, handle
        S: 32 bits, length of payload (unsigned)
        S: length bytes of payload data (if length is nonzero)
        """
        flags, type, handle, length = self._recv_fmt("!HHQI")

        if handle != cmd.handle:
            raise UnexpectedHandle(handle, cmd.handle)

        if type == REPLY_TYPE_ERROR:
            self._handle_error_chunk(length, flags)

        if type == REPLY_TYPE_ERROR_OFFSET:
            self._handle_error_offset_chunk(length, cmd)
        elif type == REPLY_TYPE_NONE:
            self._handle_none_chunk(flags, length)
        elif type == REPLY_TYPE_OFFSET_DATA:
            self._handle_data_chunk(length, cmd)
        elif type == REPLY_TYPE_OFFSET_HOLE:
            self._handle_hole_chunk(length, cmd)
        elif type == REPLY_TYPE_BLOCK_STATUS:
            self._handle_block_status_chunk(length, cmd)
        else:
            raise ProtocolError(
                "Received unknown chunk type={} flags={} length={}"
                .format(type, flags, length))

        return flags & REPLY_FLAG_DONE

    def _handle_block_status_chunk(self, length, cmd):
        """
        Receive block status chunk and populate cmd's reply dict.

        The payload starts with 32 bits, metadata context ID
        and is followed by a list of one or more Extent descriptors.
        """
        ctx_id_size = 4
        extents_count, reminder = divmod(length, Extent.size)

        if extents_count == 0 or reminder != ctx_id_size:
            raise ProtocolError(
                "Received invalid payload length {}"
                .format(length))

        if extents_count > MAX_EXTENTS:
            raise ProtocolError(
                "Received too many extents {} > {}"
                .format(extents_count, MAX_EXTENTS))

        ctx_id = self._recv_fmt("!I")[0]

        ctx_name = self._meta_context.get(ctx_id)
        if ctx_name is None:
            raise ProtocolError(
                "Received unexpected metadata context id {}".format(ctx_id))

        if ctx_name == self.dirty_bitmap:
            context = Extent.DIRTY
        elif ctx_name == QEMU_ALLOCATION_DEPTH:
            context = Extent.DEPTH
        else:
            context = Extent.ALLOC

        extents = []
        for ext in self._recv_extents(length - ctx_id_size, context):
            if ext.length % self.minimum_block_size:
                raise ProtocolError(
                    "Invalid extent length {}: not an integer multiple "
                    "of minimum block size {}"
                    .format(ext.length, self.minimum_block_size))
            extents.append(ext)

        cmd.reply[ctx_name] = extents

    def _recv_extents(self, length, context):
        """
        Iterator receiving and unpacking extents descriptors form block status
        payload.

        Yield Extent object for every extent descriptor.
        """
        # We don't expect many extents in 4 GiB, so 1024 extents per call
        # should be more than enough.
        buf = bytearray(min(length, Extent.size * 1024))

        while length:
            # Shrink buffer for the last receive.
            if length < len(buf):
                buf = memoryview(buf)[:length]

            # Receive next buffer.
            self._recv_into(buf)
            length -= len(buf)

            # Unpack extents in buffer.
            view = memoryview(buf)
            while len(view):
                yield Extent.unpack(view[:Extent.size], context)
                view = view[Extent.size:]

    def _handle_none_chunk(self, flags, length):
        if not flags & REPLY_FLAG_DONE:
            raise ProtocolError(
                "Invalid none reply chunk without done flag type={} flags={}"
                .format(REPLY_TYPE_NONE, flags))
        if length != 0:
            raise InvalidLength(REPLY_TYPE_NONE, length, 0)

    def _handle_error_chunk(self, length, flags):
        """
        Handle general error (entire request failed).

        If this the last chunk raise ReplyError, failing this request.
        Otherwise raise ProtocolError failing entire connection.

        32 bits: error (MUST be nonzero)
        16 bits: message length (no more than header length - 6)
        message length bytes: optional string suitable for direct display to a
            human being
        """
        code, message = self._recv_error_chunk(length)

        if flags & REPLY_FLAG_DONE:
            raise ReplyError(code, message)
        else:
            raise ProtocolError(
                "Unrecoverable error chunk code={} message={!r}"
                .format(code, message))

    def _handle_error_offset_chunk(self, length, cmd):
        """
        Handle error at offset (partial error). This may not be the last chunk,
        so we collect the error and continue to read the next chunk.

        32 bits: error (MUST be nonzero)
        16 bits: message length (no more than header length - 14)
        message length bytes: optional string suitable for direct display to a
            human being
        64 bits: offset (unsigned)
        """
        code, message = self._recv_error_chunk(length - 8)
        offset = self._recv_fmt("!Q")[0]
        cmd.errors.append((offset, ReplyError(code, message)))

    def _handle_data_chunk(self, length, cmd):
        """
        Receive data chunk payload into cmd's buf.

        64 bits: offset (unsigned)
        length - 8 bytes: data
        """
        # TODO: Validate that chunk offset and size are within requested range.
        chunk_offset = self._recv_fmt("!Q")[0]
        chunk_size = length - 8

        log.debug("Receive data chunk offset=%s size=%s",
                  chunk_offset, chunk_size)

        buf_offset = chunk_offset - cmd.offset
        with memoryview(cmd.buf)[buf_offset:buf_offset + chunk_size] as view:
            self._recv_into(view)

    def _handle_hole_chunk(self, length, cmd):
        """
        Handle hole chunk, zeroing byte range in cmd's buf.

        64 bits: offset (unsigned)
        32 bits: hole size (unsigned, MUST be nonzero)
        """
        if length != 12:
            raise InvalidLength(REPLY_TYPE_OFFSET_HOLE, length, 12)

        chunk_offset, chunk_size = self._recv_fmt("!QI")
        if chunk_size == 0:
            raise ProtocolError("Invalid hole chunk with zero size")

        log.debug("Receive hole chunk offset=%s size=%s",
                  chunk_offset, chunk_size)

        buf_offset = chunk_offset - cmd.offset
        cmd.buf[buf_offset:buf_offset + chunk_size] = b"\0" * chunk_size

    def _recv_error_chunk(self, length):
        code, msg_len = self._recv_fmt("!IH")

        if msg_len != length - 6:
            raise ProtocolError(
                "Invalid structure reply error message length {}, expected={}"
                .format(msg_len, length - 6))

        # The protocol does not specify the encoding.
        message = self._recv(msg_len).decode("utf-8", errors="replace")
        return code, message

    # Structured I/O

    def _recv_fmt(self, fmt):
        s = struct.Struct(fmt)
        data = self._recv(s.size)
        return s.unpack(data)

    # Plain I/O

    def _send(self, data):
        self._sock.sendall(data)

    def _recv(self, length):
        buf = bytearray(length)
        self._recv_into(buf)
        return buf

    def _recv_into(self, buf):
        with memoryview(buf) as view:
            length = len(view)
            pos = 0
            while pos < length:
                n = self._sock.recv_into(view[pos:])
                if not n:
                    raise ProtocolError(
                        "Server closed the connection, read {} bytes, "
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


class Command:
    """
    Abstract NBD command.

    All commands share the same wire format:

        32 bits, 0x25609513, magic (REQUEST_MAGIC)
        16 bits, command flags
        16 bits, type
        64 bits, handle
        64 bits, offset (unsigned)
        32 bits, length (unsigned)
    """

    wire_format = struct.Struct("!IHHQQI")

    # Attributes defined by sub classes.
    name = None
    type = None
    buf = None

    def __init__(self, handle, offset=0, length=0, flags=0):
        self.handle = handle
        self.offset = offset
        self.length = length
        self.flags = flags
        self.only_structured = False
        # NBD_REPLY_TYPE_ERROR_OFFSET chunks received when handling structued
        # reply. Can happen only in Read, Write, and BlockStatus.
        self.errors = []

    def to_bytes(self):
        return self.wire_format.pack(
            REQUEST_MAGIC,
            self.flags,
            self.type,
            self.handle,
            self.offset,
            self.length)

    def __str__(self):
        return "{} handle={} offset={} length={} flags={}".format(
            self.name, self.handle, self.offset, self.length, self.flags)


class Read(Command):
    type = 0
    name = "NBD_CMD_READ"

    def __init__(self, handle, offset, buf, only_structured=False):
        super().__init__(handle, offset, len(buf))
        # Buffer for storing the payload from the server.
        self.buf = buf
        self.only_structured = only_structured


class Write(Command):
    type = 1
    name = "NBD_CMD_WRITE"


class Disc(Command):
    type = 2
    name = "NBD_CMD_DISC"

    def __init__(self, handle):
        super().__init__(handle, 0, 0)


class Flush(Command):
    type = 3
    name = "NBD_CMD_FLUSH"

    def __init__(self, handle):
        super().__init__(handle, 0, 0)


class WriteZeroes(Command):
    type = 6
    name = "NBD_CMD_WRITE_ZEROES"


class BlockStatus(Command):
    type = 7
    name = "NBD_CMD_BLOCK_STATUS"

    def __init__(self, handle, offset, length):
        super().__init__(handle, offset, length)
        self.only_structured = True
        # Mapping of meta context name to list of Extent objects.
        self.reply = {}


class Extent:
    """
    A mutable extent of data or zeroes.

    The length field is mutable to allow merging consecutive extents received
    from the server, and splitting large extents when trimming extents that
    exceed the requested range.

    The flags field is read-only since we don't have a use case for changing
    it.

    Since this class is mutable, it must not implement __hash__.
    """

    # Contexts ids copied from qemu-nbd.
    ALLOC = 0  # base:allocation
    DEPTH = 1  # qemu:allocation-depth
    DIRTY = 2  # qemu:dirty-bitmap:name

    __slots__ = ("length", "_flags")

    # 32 bits, length of the extent to which the status below applies
    #     (unsigned, MUST be nonzero)
    # 32 bits, status flags
    wire_format = struct.Struct("!II")

    size = wire_format.size

    def __init__(self, length, flags):
        self.length = length
        self._flags = flags

    @classmethod
    def unpack(cls, data, context=ALLOC):
        """
        Create extent instance from extent data.

        Based on context, we map NBD flags bits to private bits to allow
        merging different types of extents.
        """
        length, flags = cls.wire_format.unpack(data)
        if length == 0:
            raise ProtocolError(
                "Invalid extent length=0 flags={}".format(flags))

        log.debug("Extent length=%s flags=%s context=%s",
                  length, flags, context)

        if context == cls.ALLOC:
            # The remainder of the flags field is reserved. Servers SHOULD set
            # it to all-zero; clients MUST ignore unknown flags.
            flags = flags & (STATE_HOLE | STATE_ZERO)
        elif context == cls.DIRTY:
            flags = EXTENT_DIRTY if flags & STATE_DIRTY else 0
        elif context == cls.DEPTH:
            flags = EXTENT_BACKING if flags == 0 else 0

        return cls(length, flags)

    @classmethod
    def pack(cls, length, flags):
        return cls.wire_format.pack(length, flags)

    @property
    def flags(self):
        return self._flags

    @property
    def zero(self):
        """
        For base:allocation extent, True if extents will read as zeros.
        """
        return bool(self._flags & STATE_ZERO)

    @property
    def hole(self):
        """
        Return True if this is a non existing extent in qcow2 image. If the
        image has a backing file the content of the extent are read from the
        backing file.

        Return False if the extent exist in the one of the layers, or if the
        image is a raw image, even if this is unallocated area in a raw
        image.

        By definition, a hole has also the STATE_HOLE | STATE_ZERO
        flags. But the oposite is not true.

        For more info see
        https://lists.nongnu.org/archive/html/qemu-block/2021-06/msg00756.html
        """
        return bool(self._flags & EXTENT_BACKING)

    @property
    def dirty(self):
        """
        For qemu:dirty-bitmap extent, True if extents was modified and is
        included in this incremental backup.
        """
        return bool(self._flags & EXTENT_DIRTY)

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.length == other.length and
                self.flags == other.flags)

    def __repr__(self):
        return "Extent(length={}, flags={})".format(self.length, self._flags)
