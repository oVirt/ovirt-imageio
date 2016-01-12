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


class Config(object):
    # Configuration for test connections to the proxy
    # (imaged connection configuration is in CONFIG_FILE)
    host = "127.0.0.1"
    port = 8081
    key_file = os.path.join(TEST_DIR, "pki/keys/vdsmkey.pem")
    cert_file = os.path.join(TEST_DIR, "pki/certs/vdsmcert.pem")
    use_ssl = False

    def __init__(self, imaged_config):
        self.imaged_config = imaged_config


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
    config._set('engine_cert', os.path.join(TEST_DIR, config.engine_cert))
    config._set('signing_cert', os.path.join(TEST_DIR, config.signing_cert))
    config._set('signing_key', os.path.join(TEST_DIR, config.signing_key))
    config._set('use_ssl', False)

    server_instance = server.Server()
    server_instance.start(config)
    request.addfinalizer(server_instance.stop)

    return Config(config)


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
        ["OPTIONS", {}, None, None, {}],
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
    # Verify headers required for CORS compliance
    request_headers = {
        "Authorization": signed_ticket,
        "Access-Control-Request-Headers":
            "content-range, pragma, cache-control,"
            + " authorization, content-type",
        "Access-Control-Request-Method": "PUT",
        "Host": "localhost:8081",
        "Origin": "http://localhost:" + str(proxy_server.imaged_config.port),
    }
    request_headers.update(extra_headers)
    path = "/images/" + AUTH_TICKET_ID

    with requests_mock.Mocker() as m:
        m.register_uri(method, IMAGED_URI + path,
                       status_code=200,
                       text=response_body,
                       headers=response_headers)
        res = http_request(proxy_server, method, path,
                           headers=request_headers, body=body)

    assert res.status == 200
    assert (set(item.lower() for item
            in res.getheader("access-control-allow-headers").split(", ")) ==
            {"cache-control", "pragma", "authorization", "content-type",
            "content-length", "content-range", "range"})
    assert (set(res.getheader("access-control-allow-methods").split(", ")) ==
            {"OPTIONS", "GET", "PUT", "PATCH"})
    assert (set(item.lower() for item
                in res.getheader("access-control-expose-headers")
                .split(", ")) ==
            {"authorization", "content-length", "content-range", "range"})
    assert res.getheader("access-control-allow-origin") == "*"
    assert res.getheader("access-control-max-age") == "300"


def test_images_get_imaged_200_ok(proxy_server, signed_ticket):
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket,
        "Accept-Ranges": "bytes",
        "Range": "bytes=2-6",
    }
    response_headers = {
        "Content-Range": "bytes 2-6/8",
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


def http_request(proxy_server, method, uri, body=None, headers=None):
    if proxy_server.use_ssl:
        con = httplib.HTTPSConnection(proxy_server.host,
                                      proxy_server.port,
                                      proxy_server.key_file,
                                      proxy_server.cert_file,
                                      timeout=3)
    else:
        con = httplib.HTTPConnection(proxy_server.host,
                                     proxy_server.port,
                                     timeout=3)
    with closing(con):
        con.request(method, uri, body=body, headers=headers or {})
        return response(con)


def response(con):
    res = con.getresponse()
    pprint((res.status, res.reason))
    pprint(res.getheaders())
    if res.status >= 400:
        print(res.read())
    return res
