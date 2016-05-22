

import httplib
import logging
import uuid

import requests
from webob import exc

from http_helper import (
    addcors,
    requiresession,
    success_codes as http_success_codes,
)
import session
import server

from ovirt_imageio_common import web


class ImageHandler(object):
    """
    Request handler for the /images/ resource.
    """

    def __init__(self, config, request):
        """
        :param config: config.py
        :param request: http request
        :return:
        """
        self.config = config
        self.request = request

    @addcors
    def options(self, res_id):
        return web.response(httplib.NO_CONTENT)

    @requiresession
    @addcors
    def get(self, res_id):
        resource_id = self.get_resource_id(self.request)
        imaged_url = self.get_imaged_url(self.request)

        headers = self.get_default_headers(resource_id)
        # Note that webob request.headers is case-insensitive.
        if 'Range' in self.request.headers:
            headers['Range'] = self.request.headers['Range']

        body = ""
        stream = True  # Don't let Requests read entire body into memory

        imaged_response = self.make_imaged_request(
            self.request.method, imaged_url, headers, body, stream)

        response = server.response(imaged_response.status_code)
        response.headers['Cache-Control'] = 'no-cache, no-store'
        response.headers['Content-Range'] = \
            imaged_response.headers.get('Content-Range', '')

        max_transfer_bytes = int(imaged_response.headers.get('Content-Length'))
        response.body_file = CappedStream(RequestStreamAdapter(
            imaged_response.iter_content(4096, False)),
            max_transfer_bytes)
        response.headers['Content-Length'] = str(max_transfer_bytes)
        logging.debug("Resource %s: transferring %d bytes from vdsm-imaged",
                      resource_id, max_transfer_bytes)

        return response

    @requiresession
    @addcors
    def put(self, res_id):
        return self.send_data(self.request)

    @requiresession
    @addcors
    def patch(self, res_id):
        return self.send_data(self.request)

    def send_data(self, request):
        """ Handles sending data to ovirt-imageio-daemon for PUT or PATCH.
        :param request: http request object
        :type request: webob.Request
        :return: http response object
        :rtype: webob.Response
        """
        # For now we require range headers; we could lift this restriction
        # later.  If so, be sure to add conditions to request.headers access
        # below.
        # Note that webob request.headers is case-insensitive.
        if 'Content-Range' not in request.headers:
            raise exc.HTTPBadRequest(
                "Content-Range header required for {} requests"
                .format(request.method)
            )

        resource_id = self.get_resource_id(request)
        imaged_url = self.get_imaged_url(request)

        headers = self.get_default_headers(resource_id)
        headers['Content-Range'] = request.headers['Content-Range']
        headers['Content-Length'] = request.headers['Content-Length']
        max_transfer_bytes = int(headers['Content-Length'])

        # The Requests documentation states that streaming uploads are
        # supported if data is a "file-like" object.  It looks for an
        # __iter__ attribute, then passes data along to HTTPConnection
        # .request(), where we find that all we need is a read() method.
        body = CappedStream(request.body_file, max_transfer_bytes)
        stream = False
        logging.debug("Resource %s: transferring %d bytes to vdsm-imaged",
                      resource_id, max_transfer_bytes)
        imaged_response = self.make_imaged_request(
            request.method, imaged_url, headers, body, stream)

        response = server.response(imaged_response.status_code)
        response.headers['Cache-Control'] = 'no-cache, no-store'

        return response

    def get_resource_id(self, request):
        resource_id = request.path_info_pop()
        if request.path_info:
            # No extra url path allowed!
            raise exc.HTTPBadRequest("Invalid resource path")

        # The requested image resource must match the one in the ticket
        try:
            uuid.UUID(resource_id)
        except ValueError:
            raise exc.HTTPBadRequest(
                "Invalid format for requested resource or no resource specified"
            )
        if (resource_id != session.get_session_attribute(
                request, session.SESSION_TRANSFER_TICKET)):
            raise exc.HTTPBadRequest(
                "Requested resource must match transfer ticket"
            )
        return resource_id

    def get_imaged_url(self, request):
        uri = session.get_session_attribute(request,
                                            session.SESSION_IMAGED_HOST_URI)
        ticket = session.get_session_attribute(request,
                                               session.SESSION_TRANSFER_TICKET)
        return "{}/images/{}".format(uri, ticket)

    def get_default_headers(self, resource_id):
        return {
            # accept-charset is only needed if you have query params
            'Cache-Control': 'no-cache',
            'X-AuthToken': resource_id,
        }

    def make_imaged_request(self, method, imaged_url, headers, body, stream):
        # TODO SSL (incl cert verification option)
        verify = False
        cert = None
        timeout = (self.config.imaged_connection_timeout_sec,
                   self.config.imaged_read_timeout_sec)

        logging.debug("Connecting to vdsm-imaged at %s", imaged_url)
        logging.debug("Outgoing headers to vdsm-imaged:\n" +
                      '\n'.join(('  {}: {}'.format(k, headers[k])
                                 for k in sorted(headers))))

        try:
            # TODO Pool requests, keep the session somewhere?
            # TODO Otherwise, we can use request.prepare()
            imaged_session = requests.Session()
            imaged_req = requests.Request(
                method, imaged_url, headers=headers, data=body)
            imaged_req.body_file = body
            # TODO log the request to vdsm
            imaged_prepped = imaged_session.prepare_request(imaged_req)
            imaged_resp = imaged_session.send(
                imaged_prepped, verify=verify, cert=cert,
                timeout=timeout, stream=stream)
        except requests.Timeout:
            s = "Timed out connecting to vdsm-imaged"
            raise exc.HTTPGatewayTimeout(s)
        except requests.URLRequired:
            s = "Invalid host URI for vdsm-imaged"
            raise exc.HTTPBadRequest(s)
        except requests.ConnectionError as e:
            s = "Failed communicating with vdsm-imaged: " + e.__doc__
            logging.error(s, exc_info=True)
            raise exc.HTTPServiceUnavailable(s)
        except requests.RequestException as e:
            s = "Failed communicating with vdsm-imaged: " + e.__doc__
            logging.error(s, exc_info=True)
            raise exc.HTTPInternalServerError(s)

        logging.debug("Incoming headers from vdsm-imaged:\n" +
                      '\n'.join(('  {}: {}'
                                 .format(k, imaged_resp.headers.get(k))
                                 for k in sorted(imaged_resp.headers))))

        if imaged_resp.status_code not in http_success_codes:
            # Don't read the whole body, in case something went really wrong...
            s = next(imaged_resp.iter_content(256, False), "(empty)")
            logging.error("Failed: %s", s)
            # TODO why isn't the exception logged somewhere?
            raise exc.status_map[imaged_resp.status_code](
                "Failed response from vdsm-imaged: {}".format(s))

        logging.debug(
            "Successful request to vdsm-imaged: HTTP %d %s",
            imaged_resp.status_code,
            httplib.responses[imaged_resp.status_code]
        )
        return imaged_resp


class CappedStream(object):
    """
    File-like stream wrapper limiting the amount of data transferred to avoid
    exploits or resource exhaustion from streaming more data than specified
    by a content-length header.  Its read() method will return EOF after
    max_bytes.
    """
    def __init__(self, input_stream, max_bytes):
        self.input_stream = input_stream
        self.max_bytes = max_bytes
        self.bytes_read = 0

    def __iter__(self):
        return CappedStreamIterator(self.input_stream, self.max_bytes)

    def read(self, size):
        to_read = min(size, self.max_bytes - self.bytes_read)
        self.bytes_read += to_read
        return self.input_stream.read(to_read)


class CappedStreamIterator(object):
    """ Iterator for CappedStream object. """
    chunk_size = 4096

    def __init__(self, input_stream, max_bytes):
        self.capped_stream = CappedStream(input_stream, max_bytes)

    def __iter__(self):
        return self

    def next(self):
        output = self.capped_stream.read(self.chunk_size)
        if not output:
            raise StopIteration
        return output


class RequestStreamAdapter(object):
    """
    Converts Request.iter_content stream to have a standard read() interface
    """
    chunk_size = 4096

    def __init__(self, request_stream):
        self.generator = request_stream
        self.next_bytes = b''

    def read(self, size):
        ret = self.next_bytes
        while len(ret) < size:
            try:
                ret += self.generator.next()
            except StopIteration:
                break
        self.next_bytes = ret[size:]
        return ret[:size]
