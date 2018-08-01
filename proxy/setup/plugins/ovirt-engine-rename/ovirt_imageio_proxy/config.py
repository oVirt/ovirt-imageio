#
# ovirt-engine-setup -- ovirt engine setup
# Copyright (C) 2016 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""ovirt-imageio-proxy plugin."""


import gettext
import os
import textwrap


from otopi import constants as otopicons
from otopi import filetransaction
from otopi import plugin
from otopi import util

from ovirt_engine_setup import constants as osetupcons
from ovirt_engine_setup.engine import constants as oenginecons
from ovirt_engine_setup.engine import vdcoption
from ovirt_engine_setup.ovirt_imageio_proxy import constants as oipcons


@util.export
class Plugin(plugin.PluginBase):
    """ovirt-imageio-proxy plugin."""

    def __init__(self, context):
        super(Plugin, self).__init__(context=context)

    @plugin.event(
        stage=plugin.Stages.STAGE_MISC,
        condition=lambda self: (
            self.environment[
                oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG
            ] and
            self.environment[
                oenginecons.CoreEnv.ENABLE
            ]
        ),
    )
    def _databaseOptions(self):
        ImageProxyName = vdcoption.VdcOption(
            statement=self.environment[
                oenginecons.EngineDBEnv.STATEMENT
                ]
        ).getVdcOption(
            'ImageProxyAddress',
            ownConnection=True,
        ),

        old_fqdn, old_port = ImageProxyName[0].split(":")
        if old_fqdn != self.environment[osetupcons.ConfigEnv.FQDN]:
            vdcoption.VdcOption(
                statement=self.environment[
                    oenginecons.EngineDBEnv.STATEMENT
                ]
            ).updateVdcOptions(
                options=(
                    {
                        'name': 'ImageProxyAddress',
                        'value': '%s:%s' % (
                            self.environment[osetupcons.ConfigEnv.FQDN],
                            old_port
                        ),
                    },
                ),
            )
            self.logger.info(
                _("ImageProxyAddress has been changed to:\n"
                    "	{fqdn}:{port}\n"
                  ).format(
                    fqdn=self.environment[osetupcons.ConfigEnv.FQDN],
                    port=old_port
                ),
            )

# vim: expandtab tabstop=4 shiftwidth=4
