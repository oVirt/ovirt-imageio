
from functools import wraps
import logging
import re

from webob.util import status_reasons

import session

# Content-range (eg "bytes 0-1023/4096" or "bytes 0-15/*")
cr_regex = re.compile(r"bytes (\d+)-(\d+)/((?:\d+)|(?:\*))", re.IGNORECASE)


def httplog(func):
    @wraps(func)
    def wrapper(self, request):
        content_length = request.headers.get('Content-Length', '0')
        logging.info('%s %s %s %s',
                     request.method.upper(),
                     request.url,
                     request.http_version,
                     content_length)
        if request.headers:
            logging.debug("Request headers:\n" +
                          '\n'.join(('  {}: {}'.format(h, request.headers[h])
                                     for h in sorted(request.headers))))

        ret = func(self, request)

        success = int(ret.status_code / 100) == 2
        # TODO make sure this doesn't cause body enumeration
        content_length = ret.headers.get('Content-Length', '*')
        log_method = logging.info if success else logging.error
        log_method("%s %s %s %d %s (%s)",
                   request.method.upper(),
                   request.url,
                   request.http_version,
                   ret.status_code,
                   content_length,
                   status_reasons.get(ret.status_code, 'unknown'))
        if not success:
            log_method(ret.text[:256].rstrip())
        logging.debug("Response headers:\n" +
                      '\n'.join(('  {}: {}'.format(h, ret.headers[h])
                                 for h in sorted(ret.headers))))
        return ret
    return wrapper


def addcors(func):
    @wraps(func)
    def wrapper(self, request):
        ret = func(self, request)
        ret.headers.add("Access-Control-Allow-Origin", "*")
        ret.headers.add("Access-Control-Allow-Headers",
                        "Cache-Control, Pragma, Authorization, Content-Type,"
                        " Content-Length, Content-Range, Range")
        ret.headers.add("Access-Control-Allow-Methods",
                        "GET, PUT, PATCH, OPTIONS")
        ret.headers.add("Access-Control-Expose-Headers",
                        "Authorization, Content-Length, Content-Range, Range")
        ret.headers.add("Access-Control-Max-Age", "300")
        return ret
    return wrapper


def requiresession(func):
    """
    Annotation to wrap an HTTP method to ensure a session is loaded, returning
    a failed HTTP response if one could not be established.

    :param func: HTTP method to wrap
    :return: webob response object, either failure (if session could not be initialized) or HTTP method return value
    :raise webob.exc.HTTPException: Error creating session or running HTTP method
    """
    @wraps(func)
    def wrapper(self, request):
        session.start_session(request)

        try:
            ret = func(self, request)
        except Exception as e:
            session.update_session_activity(request)
            raise

        session.update_session_activity(request)
        return ret
    return wrapper


def parse_content_range(content_range):
    """
    Parse a Content-Range header eg "bytes 0-15/100"

    :raise ValueError: header could not be parsed
    :return: tuple of (int start, int end, int total or str '*', int count)
    """
    m = cr_regex.match(content_range)
    if m is None:
        raise ValueError("Invalid content range")
    else:
        r = m.groups()
        r = (int(r[0]), int(r[1]), r[2] if r[2] == '*' else int(r[2]))
        return r + (r[1] - r[0] + 1,)
