
import errno
import httplib
import os
import socket
import ssl
from wsgiref import simple_server


class UnsupportedError(Exception):
    pass


class _UnixMixin(object):

    def set_tunnel(self, host, port=None, headers=None):
        raise UnsupportedError("Tunneling is not supported")


class UnixHTTPConnection(_UnixMixin, httplib.HTTPConnection):
    """
    HTTP connection over unix domain socket.
    """

    def __init__(self, path, strict=None,
                 timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.path = path
        httplib.HTTPConnection.__init__(self, "localhost", strict=strict,
                                        timeout=timeout)

    def connect(self):
        self.sock = _create_unix_socket(self.timeout)
        self.sock.connect(self.path)


class UnixHTTPSConnection(_UnixMixin, httplib.HTTPSConnection):
    """
    HTTPS connection over unix domain socket.
    """

    def __init__(self, path, key_file=None, cert_file=None,
                 strict=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.path = path
        httplib.HTTPSConnection.__init__(self, "localhost", key_file=key_file,
                                         cert_file=cert_file, strict=strict,
                                         timeout=timeout)

    def connect(self):
        sock = _create_unix_socket(self.timeout)
        self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file)
        self.sock.connect(self.path)


class UnixWSGIServer(simple_server.WSGIServer):
    """
    WSGI HTTP server over unix domain socket.
    """

    address_family = socket.AF_UNIX
    server_name = "localhost"
    server_port = None

    def server_bind(self):
        """
        Override to remove existing socket.
        """
        try:
            os.unlink(self.server_address)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        self.socket.bind(self.server_address)
        self.setup_environ()

    def get_request(self):
        """
        Override to return non-empty client address, expected by
        WSGIRequestHandler.
        """
        sock, _ = self.socket.accept()
        return sock, self.server_address


class UnixWSGIRequestHandler(simple_server.WSGIRequestHandler):
    """
    WSGI HTTP request handler over unix domain socket.
    """

    def address_string(self):
        """
        Override to avoid pointless code in WSGIRequestHandler assuming AF_INET
        socket address (host, port).
        """
        return "localhost"


def _create_unix_socket(timeout):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
        sock.settimeout(timeout)
    return sock
