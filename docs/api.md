# ovirt-imageio-proxy

/sessions
---------

POST

Request:
```
POST /sessions HTTP/1.1
Authorization: <signed ticket data>
```

Response:
```
HTTP/1.1 200 OK
Session-Id: <session id>
```

DELETE

Request:
```
DELETE /sessions/<session id> HTTP/1.1
Authorization: <signed ticket data>
```
Response:
```
HTTP/1.1 204 No Content
```

/images
-------

Write me

/info
~~~~~

Request:
```
Get /info/ HTTP/1.1
```
Response:
```
HTTP/1.1 200 OK
content-type: application/json

{
    "version": "1.2.0"
}
```
