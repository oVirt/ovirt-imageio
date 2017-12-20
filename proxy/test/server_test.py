import pytest
import requests_mock

from ovirt_imageio_common.ssl import check_protocol
from ovirt_imageio_proxy import auth

from . import http


def test_connect(proxy_server):
    res = http.request(proxy_server, "OPTIONS", "/")
    assert res.status == 404


def test_no_resource(proxy_server):
    res = http.request(proxy_server, "PUT", "/no/such/resource")
    assert res.status == 404


def test_images_no_auth(proxy_server):
    res = http.request(proxy_server, "GET", "/images/")
    assert res.status == 401


def test_images_no_auth_invalid_session_id_param(proxy_server):
    res = http.request(proxy_server, "GET",
                       "/images/missing_ticket?session_id=missing_session_id")
    assert res.status == 401


def test_images_no_auth_invalid_session_id_header(proxy_server):
    res = http.request(proxy_server, "GET","/images/missing_ticket",
                       headers={"Session-Id ": "missing"})
    assert res.status == 401


def test_images_unparseable_auth(proxy_server):
    headers = {"Authorization": 'test'}
    res = http.request(proxy_server, "GET", "/images/", headers=headers)
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
    request_headers = images_request_headers(signed_ticket.data)
    request_headers.update(extra_headers)
    path = "/images/" + signed_ticket.id

    with requests_mock.Mocker() as m:
        m.register_uri(method, signed_ticket.url + path,
                       status_code=200,
                       text=response_body,
                       headers=response_headers)
        res = http.request(proxy_server, method, path,
                           headers=request_headers, body=body)

    allowed_headers = split_header(res.getheader("access-control-expose-headers"))
    expected_headers = {"authorization", "content-length", "content-range", "range", "session-id"}

    assert res.status == 200
    assert allowed_headers == expected_headers
    assert res.getheader("access-control-allow-origin") == "*"


def split_header(s):
    return set(value.strip().lower() for value in s.split(","))


def test_images_cors_options(proxy_server, signed_ticket):
    request_headers = images_request_headers(signed_ticket.data)
    path = "/images/" + signed_ticket.id

    res = http.request(proxy_server, "OPTIONS", path,
                       headers=request_headers, body=None)

    allowed_headers = split_header(res.getheader("access-control-allow-headers"))
    expected_headers = {"cache-control", "pragma", "authorization", "content-type",
                        "content-length", "content-range", "range", "session-id"}

    allowed_methods = split_header(res.getheader("access-control-allow-methods"))
    expected_methods = {"options", "get", "put", "patch", "post", "delete"}

    assert res.status == 204
    assert allowed_headers == expected_headers
    assert allowed_methods == expected_methods
    assert res.getheader("access-control-allow-origin") == "*"
    assert res.getheader("access-control-max-age") == "300"


def test_images_get_imaged_with_authorization(proxy_server, signed_ticket):
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket.data,
        "Accept-Ranges": "bytes",
        "Content-Length": "0",
        "Range": "bytes=2-6",
    }
    response_headers = {
        "Content-Range": "bytes 2-6/8",
        "Content-Length": "5",
        "Content-Disposition": "attachment; filename=\xd7\x90", # this is hebrew aleph
    }
    path = "/images/" + signed_ticket.id

    with requests_mock.Mocker() as m:
        m.get(signed_ticket.url + path,
              status_code=200,
              text=body,
              headers=response_headers)
        res = http.request(proxy_server, "GET", path, headers=request_headers)
        assert m.called
    assert res.status == 200
    assert res.read() == "hello"
    assert res.getheader("content-length") == "5"
    assert res.getheader("content-disposition") == "attachment; filename=\xd7\x90"


def test_images_get_imaged_with_installed_ticket(proxy_server, signed_ticket):
    auth.add_signed_ticket(signed_ticket.data)

    body = "hello"
    request_headers = {
        "Accept-Ranges": "bytes",
    }
    response_headers = {
        "Content-Length": "5",
    }
    path = "/images/" + signed_ticket.id

    with requests_mock.Mocker() as m:
        m.get(signed_ticket.url + path,
              status_code=200,
              text=body,
              headers=response_headers)
        res = http.request(proxy_server, "GET", path, headers=request_headers)
        assert m.called
    assert res.status == 200
    assert res.read() == "hello"
    assert res.getheader("content-length") == "5"


def test_images_get_imaged_401_unauthorized(proxy_server, signed_ticket):
    # i.e. imaged doesn't have a valid ticket for this request
    request_headers = {
        "Authorization": signed_ticket.data,
        "Accept-Ranges": "bytes",
        "Range": "bytes=0-5",
    }
    path = "/images/" + signed_ticket.id

    with requests_mock.Mocker() as m:
        m.get(signed_ticket.url + path,
              status_code=401,
              text="Unauthorized")
        res = http.request(proxy_server, "GET", path, headers=request_headers)
        assert m.called
    assert res.status == 401


def test_images_get_imaged_404_notfound(proxy_server, signed_ticket):
    # i.e. imaged can't find this resource
    request_headers = {
        "Authorization": signed_ticket.data,
        "Accept-Ranges": "bytes",
        "Range": "bytes=0-5",
    }
    path = "/images/" + signed_ticket.id

    with requests_mock.Mocker() as m:
        m.get(signed_ticket.url + path,
              status_code=404,
              text="Not found")
        res = http.request(proxy_server, "GET", path, headers=request_headers)
        assert m.called
    assert res.status == 404


def test_images_put_imaged_200_ok(proxy_server, signed_ticket):
    body = "hello"
    client_headers = {
        "Authorization": signed_ticket.data,
        "Accept-Ranges": "bytes",
        "Content-Range": "bytes 2-6/10",
    }
    path = "/images/" + signed_ticket.id

    proxy_headers = {
        "Content-Length": str(len(body)),
        "Content-Range": "bytes 2-6/10",
    }

    with requests_mock.Mocker() as m:
        m.put(signed_ticket.url + path,
              status_code=200,
              text=None,
              request_headers=proxy_headers)
        res = http.request(proxy_server, "PUT", path, body=body,
                           headers=client_headers)
        assert m.called
    assert res.status == 200


def test_images_put_imaged_401_unauthorized(proxy_server, signed_ticket):
    # i.e. imaged doesn't have a valid ticket for this request
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket.data,
        "Accept-Ranges": "bytes",
        "Content-Range": "bytes 2-6/10",
    }
    path = "/images/" + signed_ticket.id

    with requests_mock.Mocker() as m:
        m.put(signed_ticket.url + path,
              status_code=401,
              text="Unauthorized")
        res = http.request(proxy_server, "PUT", path, body=body,
                           headers=request_headers)
        assert m.called
    assert res.status == 401


@pytest.mark.xfail(reason="Fails in CI, needs more work.")
def test_images_put_imaged_404_notfound(proxy_server, signed_ticket):
    # i.e. imaged can't find this resource
    body = "hello"
    request_headers = {
        "Authorization": signed_ticket.data,
        "Accept-Ranges": "bytes",
        "Content-Range": "bytes 2-6/10",
    }
    path = "/images/" + signed_ticket.id

    with requests_mock.Mocker() as m:
        m.put(signed_ticket.url + path,
              status_code=404,
              text="Not found")
        res = http.request(proxy_server, "PUT", path, headers=request_headers)
        assert m.called
    assert res.status == 404


@pytest.mark.parametrize("protocol", ["-ssl2", "-ssl3", "-tls1"])
def test_reject_protocols(proxy_server, protocol):
    rc = check_protocol("127.0.0.1", proxy_server.port, protocol)
    assert rc != 0


@pytest.mark.parametrize("protocol", ["-tls1_1", "-tls1_2"])
def test_accept_protocols(proxy_server, protocol):
    rc = check_protocol("127.0.0.1", proxy_server.port, protocol)
    assert rc == 0


def test_sessions_post_sessionid_exists(proxy_server, signed_ticket):
    client_headers = {
        "Authorization": signed_ticket.data
    }
    path = "/sessions/"

    res = http.request(proxy_server, "POST", path, headers=client_headers)
    session_id = res.getheader('Session-Id')

    client_headers['Session-Id'] = session_id
    res = http.request(proxy_server, "POST", path, headers=client_headers)

    assert res.getheader('Session-Id') == session_id


def test_images_delete_missing_session(proxy_server, signed_ticket):
    res = http.request(proxy_server, "DELETE", "/sessions/missing")
    assert res.status == 404


def images_request_headers(authorization):
    return {
       "Access-Control-Request-Headers": "content-range, pragma, "
                                         "cache-control, "
                                         "authorization, "
                                         "content-type",
       "Access-Control-Request-Method": "PUT",
       "Authorization": authorization,
       "Host": "localhost:8081",
       "Origin": "http://localhost:0000"
    }
