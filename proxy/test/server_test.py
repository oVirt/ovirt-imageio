from __future__ import print_function
from contextlib import closing
from pprint import pprint
import httplib
import logging
import os
import ssl

import pytest
import requests_mock

from ovirt_image_proxy import server

TEST_DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(TEST_DIR, "resources/test_config.ini")
SIGNED_TICKET_FILE = os.path.join(TEST_DIR, "resources/auth_ticket.out")

# From resources/auth_ticket.in values
AUTH_TICKET_ID = "f6fe1b31-1c90-4dc3-a4b9-7b02938c8b41"
IMAGED_URI = "https://localhost:54322"


# Disable client certificate verification introduced in Python > 2.7.9. We
# trust our certificates.
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass  # Older Python, not required


# TODO session scope after refactoring
@pytest.fixture
def proxy_server(request):
    logging.basicConfig(
        filename="/dev/stdout",
        level=logging.DEBUG,
        format="(%(threadName)-10s) %(levelname)s %(name)s:%(message)s")

    # Use a custom test configuration for the server; pki paths must be updated
    from ovirt_image_proxy import config
    config.load(CONFIG_FILE)
    config._set('engine_cert_file',
                os.path.join(TEST_DIR, config.engine_cert_file))
    config._set('ssl_key_file',
                os.path.join(TEST_DIR, "pki/keys/vdsmkey.pem"))
    config._set('ssl_cert_file',
                os.path.join(TEST_DIR, "pki/certs/vdsmcert.pem"))

    server_instance = server.Server()
    server_instance.start(config)
    request.addfinalizer(server_instance.stop)

    return config


@pytest.fixture
def signed_ticket():
    with open(SIGNED_TICKET_FILE, 'r') as f:
        return f.read().rstrip()


def test_connect(proxy_server):
    res = http_request(proxy_server, "OPTIONS", "/")
    assert res.status == 404


def test_no_resource(proxy_server):
    res = http_request(proxy_server, "PUT", "/no/such/resource")
    assert res.status == 404


def test_images_no_auth(proxy_server):
    res = http_request(proxy_server, "GET", "/images/")
    assert res.status == 401


def test_images_unparseable_auth(proxy_server):
    headers = {"Authorization": 'test'}
    res = http_request(proxy_server, "GET", "/images/", headers=headers)
    assert res.status == 401


@pytest.mark.parametrize(
    "method,extra_headers,body,response_body,response_headers", [
        ["GET", {"Range": "bytes=0-4", "Content-Length": "0"}, None,
         "hello", {"Content-Range": "bytes 0-4/5", "Content-Length": "5"}],
        ["PUT", {"Content-Range": "bytes 0-4/5", "Content-Length": "5"},
         "hello", None, {}],
        ["PATCH", {"Content-Range": "bytes 0-4/5", "Content-Length": "5"},
         "hello", None, {}],
    ])
def test_images_cors_compliance(proxy_server, signed_ticket,
                                method, extra_headers, body,
                                response_body, response_headers):
    request_headers = images_request_headers(signed_ticket)
    request_headers.update(extra_headers)
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.register_uri(method, IMAGED_URI + path,
                       status_code=200,
                       text=response_body,
                       headers=response_headers)
        res = http_request(proxy_server, method, path,
                           headers=request_headers, body=body)

    allowed_headers = split_header(res.getheader("access-control-expose-headers"))
    expected_headers = {"authorization", "content-length", "content-range", "range"}

    assert res.status == 200
    assert allowed_headers == expected_headers
    assert res.getheader("access-control-allow-origin") == "*"


def split_header(s):
    return set(value.strip().lower() for value in s.split(","))


def test_images_cors_options(proxy_server, signed_ticket):
    request_headers = images_request_headers(signed_ticket)
    path = "/images/" + AUTH_TICKET_ID

    res = http_request(proxy_server, "OPTIONS", path,
                       headers=request_headers, body=None)

    allowed_headers = split_header(res.getheader("access-control-allow-headers"))
    expected_headers = {"cache-control", "pragma", "authorization", "content-type",
                        "content-length", "content-range", "range"}

    allowed_methods = split_header(res.getheader("access-control-allow-methods"))
    expected_methods = {"options", "get", "put", "patch"}

    assert res.status == 204
    assert allowed_headers == expected_headers
    assert allowed_methods == expected_methods
    assert res.getheader("access-control-allow-origin") == "*"
    assert res.getheader("access-control-max-age") == "300"


def test_images_get_imaged_200_ok(proxy_server, signed_ticket):
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket,
        "Accept-Ranges": "bytes",
        "Content-Length": "0",
        "Range": "bytes=2-6",
    }
    response_headers = {
        "Content-Range": "bytes 2-6/8",
        "Content-Length": "5",
    }
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.get(IMAGED_URI + path,
              status_code=200,
              text=body,
              headers=response_headers)
        res = http_request(proxy_server, "GET", path, headers=request_headers)
        assert m.called
    assert res.status == 200
    assert res.read() == "hello"
    assert res.getheader("content-length") == "5"


def test_images_get_imaged_401_unauthorized(proxy_server, signed_ticket):
    # i.e. imaged doesn't have a valid ticket for this request
    request_headers = {
        "Authorization": signed_ticket,
        "Accept-Ranges": "bytes",
        "Range": "bytes=0-5",
    }
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.get(IMAGED_URI + path,
              status_code=401,
              text="Unauthorized")
        res = http_request(proxy_server, "GET", path, headers=request_headers)
        assert m.called
    assert res.status == 401


def test_images_get_imaged_404_notfound(proxy_server, signed_ticket):
    # i.e. imaged can't find this resource
    request_headers = {
        "Authorization": signed_ticket,
        "Accept-Ranges": "bytes",
        "Range": "bytes=0-5",
    }
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.get(IMAGED_URI + path,
              status_code=404,
              text="Not found")
        res = http_request(proxy_server, "GET", path, headers=request_headers)
        assert m.called
    assert res.status == 404


def test_images_put_imaged_200_ok(proxy_server, signed_ticket):
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket,
        "Accept-Ranges": "bytes",
        "Content-Range": "bytes 2-6/10",
    }
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.put(IMAGED_URI + path,
              status_code=200,
              text=None)
        res = http_request(proxy_server, "PUT", path, body=body,
                           headers=request_headers)
        assert m.called
    assert res.status == 200


def test_images_put_imaged_401_unauthorized(proxy_server, signed_ticket):
    # i.e. imaged doesn't have a valid ticket for this request
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket,
        "Accept-Ranges": "bytes",
        "Content-Range": "bytes 2-6/10",
    }
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.put(IMAGED_URI + path,
              status_code=401,
              text="Unauthorized")
        res = http_request(proxy_server, "PUT", path, body=body,
                           headers=request_headers)
        assert m.called
    assert res.status == 401


@pytest.mark.noci
def test_images_put_imaged_404_notfound(proxy_server, signed_ticket):
    # i.e. imaged can't find this resource
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket,
        "Accept-Ranges": "bytes",
        "Content-Range": "bytes 2-6/10",
    }
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.put(IMAGED_URI + path,
              status_code=404,
              text="Not found")
        res = http_request(proxy_server, "PUT", path, headers=request_headers)
        assert m.called
    assert res.status == 404


def images_request_headers(signed_ticket):
    return {
       "Access-Control-Request-Headers": "content-range, pragma, "
                                         "cache-control, "
                                         "authorization, "
                                         "content-type",
       "Access-Control-Request-Method": "PUT",
       "Authorization": signed_ticket,
       "Host": "localhost:8081",
       "Origin": "http://localhost:0000"
    }


def http_request(proxy_server, method, uri, body=None, headers=None):
    if proxy_server.use_ssl:
        con = httplib.HTTPSConnection("localhost",
                                      proxy_server.port,
                                      proxy_server.ssl_key_file,
                                      proxy_server.ssl_cert_file,
                                      timeout=3)
    else:
        con = httplib.HTTPConnection("localhost",
                                     proxy_server.port,
                                     timeout=3)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason))
    pprint(res.getheaders())
    return res
