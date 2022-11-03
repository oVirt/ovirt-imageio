# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import logging
import os
import yaml

import pytest


log = logging.getLogger("test")


@pytest.fixture
def config(tmpdir):
    with open(os.environ['CLIENT_TEST_CONF'], encoding='utf-8') as fstream:
        try:
            conf = yaml.safe_load(fstream)
        except yaml.YAMLError as exc:
            log.error("Invalid YAML format: %s", exc)
            raise

    conf_file = os.path.join(tmpdir, 'ovirt-img.conf')
    os.environ['XDG_CONFIG_HOME'] = str(tmpdir)
    with open(conf_file, "w+", encoding="utf-8") as fstream:
        fstream.write("[test]\n")
        for k, v in conf["common"].items():
            fstream.write(f"{k} = {v}\n")

    yield conf
