

import httplib
import logging
import uuid

import requests
from webob import exc

from http_helper import (
    parse_content_range,
    requiresession,
)
import session
import server


class ImageHandler(object):
    """
    Request handler for the /images/ resource.
    """

    def __init__(self, config):
        """
        :param config: config.py
        :return:
        """
        self.config = config

    def options(self, request):
        return server.response(httplib.OK)

    @requiresession
    def get(self, request):
        return self.handleImageDataRequest(request)

    @requiresession
    def put(self, request):  #ticket_id):
        return self.handleImageDataRequest(request)

    @requiresession
    def patch(self, request):  #ticket_id):
        return self.handleImageDataRequest(request)

    # TODO this method is a bit long, can we refactor?
    def handleImageDataRequest(self, request):
        """ Handles a request to PUT or GET data to/from vdsm-imaged
        :param request: http request object
        :type request: webob.Request
        :return: http response object
        :rtype: webob.Response
        """
        # Validate the request
        if request.method not in ('GET', 'PUT', 'PATCH'):
            raise exc.HTTPBadRequest("Method not supported")

        # For now we require range headers; we could lift this restriction
        # later.  If so, be sure to add conditions to request.headers access
        # below.
        # Note that webob request.headers is case-insensitive.
        if request.method == 'GET' and 'Range' not in request.headers:
            raise exc.HTTPBadRequest(
                    "Range header required for GET requests"
            )
        elif (request.method in ('PUT', 'PATCH') and
                'Content-Range' not in request.headers):
            raise exc.HTTPBadRequest(
                    "Content-Range header required for {} requests"
                    .format(request.method)
            )

        resource_id = request.path_info_pop()
        if request.path_info:
            # No extra url path allowed!
            raise exc.HTTPBadRequest("Invalid resource path")

        # The requested image resource must match the one in the token
        try:
            uuid.UUID(resource_id)
        except ValueError:
            raise exc.HTTPBadRequest(
                    "Invalid format for requested resource or no resource specified"
            )
        if (resource_id != session.get_session_attribute(
                request, session.SESSION_TRANSFER_TICKET)):
            raise exc.HTTPBadRequest(
                    "Requested resource must match transfer token"
            )

        request_id = uuid.uuid4()
        uri = session.get_session_attribute(request,
                                            session.SESSION_IMAGED_HOST_URI)
        if uri.startswith('http://'):
            uri = uri[7:]
        if uri.startswith('https://'):
            uri = uri[8:]
        imaged_url = "{}://{}:{}/images/{}?id={}".format(
                'https' if self.config.imaged_ssl else 'http',
                uri,
                self.config.imaged_port,
                session.get_session_attribute(request,
                                              session.SESSION_TRANSFER_TICKET),
                request_id)

        # TODO SSL (incl cert verification option)
        verify=False
        cert=None
        timeout=(self.config.imaged_connection_timeout_sec,
                 self.config.imaged_read_timeout_sec)

        headers = {}
        # accept-charset is only needed if you have query params
        headers['Cache-Control'] = 'no-cache'
        headers['X-AuthToken'] = resource_id

        if request.method == 'GET':
            headers['Range'] = request.headers['Range']
            body = ""
            stream = True  # Don't let Requests read entire body into memory

        else:  # PUT, PATCH
            headers['Content-Range'] = request.headers['Content-Range']
            try:
                max_transfer_bytes = \
                        parse_content_range(request.headers['Content-Range'])[3]
            except ValueError as e:
                raise exc.HTTPBadRequest("Invalid request: " + e.message)
            headers['Content-Length'] = max_transfer_bytes

            # The Requests documentation states that streaming uploads are
            # supported if data is a "file-like" object.  It looks for an
            # __iter__ attribute, then passes data along to HTTPConnection
            # .request(), where we find that all we need is a read() method.
            body = CappedStream(request.body_file, max_transfer_bytes)
            stream = False
            logging.debug("Resource %s: transferring %d bytes to vdsm-imaged",
                      resource_id, max_transfer_bytes)

        logging.debug("Connecting to vdsm-imaged at %s", imaged_url)
        for k in sorted(headers):
            logging.debug("Outgoing header %s: %s", k, headers[k])

        try:
            # TODO Pool requests, keep the session somewhere?
            # TODO Otherwise, we can use request.prepare()
            imaged_session = requests.Session()
            imaged_req = requests.Request(
                    request.method, imaged_url, headers=headers, data=body)
            imaged_req.body_file=body
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

        if (imaged_resp.status_code != httplib.OK
                and imaged_resp.status_code != httplib.PARTIAL_CONTENT
                and imaged_resp.status_code != httplib.NO_CONTENT):
            # Don't read the whole body, in case something went really wrong...
            s = imaged_resp.iter_content(256, False).next()
            logging.error("Failed: %s", s)
            # TODO why isn't the exception logged somewhere?
            raise exc.status_map[imaged_resp.status_code](
                    "Failed response from vdsm-imaged: {}".format(s))

        logging.debug(
                "Successful request to vdsm-imaged: HTTP %d %s",
                imaged_resp.status_code,
                httplib.responses[imaged_resp.status_code]
        )

        response = server.response(imaged_resp.status_code)
        response.headers['Cache-Control'] = 'no-cache, no-store'

        if request.method == 'GET':
            response.headers['Content-Range'] = \
                    imaged_resp.headers.get('Content-Range', '')
            try:
                max_transfer_bytes = \
                    parse_content_range(response.headers['Content-Range'])[3]
            except ValueError as e:
                raise exc.HTTPBadGateway(
                        "Invalid response from vdsm-imaged: " + e.message
                )
            response.body_file = CappedStream(RequestStreamAdapter(
                    imaged_resp.iter_content(4096, False)), max_transfer_bytes)
            logging.debug("Resource %s: transferring %d bytes from vdsm-imaged",
                          resource_id, max_transfer_bytes)

        return response


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
