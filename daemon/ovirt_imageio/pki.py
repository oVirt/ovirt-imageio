# ovirt-imageio-daemon
# Copyright (C) 2015-2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import os


def key_file(config):
    return os.path.join(config.tls.pki_dir, "keys", "vdsmkey.pem")


def cert_file(config):
    return os.path.join(config.tls.pki_dir, "certs", "vdsmcert.pem")
