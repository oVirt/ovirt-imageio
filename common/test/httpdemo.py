# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
To run the demo server:

$ python test/httpdemo.py

Using /echo endpoint:

$ curl -X PUT --upload-file fedora-27.img \
    http://localhost:8000/echo/ticket > /dev/null
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 12.0G  100 6144M  100 6144M   659M   659M  0:00:09  0:00:09 --:--:-- 1313M

Benchmarking the server with wrk[1]:

$ ./wrk -t1 -c4 --latency http://localhost:8000/bench/4
Running 10s test @ http://localhost:8000/bench/4
  1 threads and 4 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   763.47us  257.50us   5.16ms   74.47%
    Req/Sec     5.27k    95.89     5.39k    90.00%
  Latency Distribution
     50%  719.00us
     75%    0.89ms
     90%    1.09ms
     99%    1.57ms
  52410 requests in 10.00s, 5.65MB read
Requests/sec:   5240.66
Transfer/sec:    578.34KB

[1] https://github.com/wg/wrk

"""

from __future__ import absolute_import
import argparse
import logging
from ovirt_imageio_common import http
from ovirt_imageio_common import util

log = logging.getLogger("httpdemo")


class Echo(object):

    def put(self, req, resp, ticket):
        if req.headers.get("expect") == "100-continue":
            resp.send_info(http.CONTINUE)

        count = req.content_length
        resp.headers["content-length"] = count

        while count:
            with req.clock.run("read"):
                chunk = req.read(1024 * 1024)
            if not chunk:
                raise http.Error(400, "Client disconnected")
            with req.clock.run("write"):
                resp.write(chunk)
            count -= len(chunk)


class Bench(object):

    def get(self, req, resp, name):
        body = b"%s\n" % name.encode("utf-8")
        resp.headers["content-length"] = len(body)
        with req.clock.run("write"):
            resp.write(body)


parser = argparse.ArgumentParser()
parser.add_argument(
    "-d", "--debug",
    action="store_true",
    help="Debug mode")
parser.add_argument(
    "-p", "--port",
    default=8000,
    help="listen port (default 8000)")
args = parser.parse_args()

logging.basicConfig(
    level=logging.DEBUG if args.debug else logging.INFO,
    format="%(asctime)s %(levelname)-7s (%(threadName)s) %(message)s")

log.info("Starting server on port %s", args.port)

server = http.Server(("", args.port), http.Connection)
if args.debug:
    server.clock_class = util.Clock

server.app = http.Router([
    (r"/bench/(.*)", Bench()),
    (r"/echo/(.*)", Echo()),
])

server.serve_forever()
