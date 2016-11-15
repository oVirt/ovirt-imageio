# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import errno
import os
import threading


def uninterruptible(func, *args):
    while True:
        try:
            return func(*args)
        except EnvironmentError as e:
            if e.errno != errno.EINTR:
                raise


def start_thread(func, args=(), kwargs=None, name=None, daemon=True):
    if kwargs is None:
        kwargs = {}
    t = threading.Thread(target=func, args=args, kwargs=kwargs, name=name)
    t.daemon = daemon
    t.start()
    return t


def monotonic_time():
    return os.times()[4]
