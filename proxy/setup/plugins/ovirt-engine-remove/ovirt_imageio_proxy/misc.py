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

from otopi import plugin
from otopi import util

from ovirt_engine_setup import constants as osetupcons
from ovirt_engine_setup.ovirt_imageio_proxy import constants as oipcons

from ovirt_setup_lib import dialog


def _(m):
    return gettext.dgettext(message=m, domain='ovirt-imageio-proxy-setup')


@util.export
class Plugin(plugin.PluginBase):
    """ovirt-imageio-proxy plugin."""

    @plugin.event(
        stage=plugin.Stages.STAGE_INIT,
    )
    def _init(self):
        self.environment.setdefault(
            oipcons.RemoveEnv.REMOVE_IMAGEIO_PROXY,
            None
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_CUSTOMIZATION,
        after=(
            osetupcons.Stages.REMOVE_CUSTOMIZATION_COMMON,
        ),
    )
    def _customization(self):
        if self.environment[osetupcons.RemoveEnv.REMOVE_ALL]:
            self.environment[oipcons.RemoveEnv.REMOVE_IMAGEIO_PROXY] = True

        if self.environment[oipcons.RemoveEnv.REMOVE_IMAGEIO_PROXY] is None:
            self.environment[
                oipcons.RemoveEnv.REMOVE_IMAGEIO_PROXY
            ] = dialog.queryBoolean(
                dialog=self.dialog,
                name='OVESETUP_REMOVE_IMAGEIO_PROXY',
                note=_(
                    'Do you want to remove the Image I/O Proxy? '
                    '(@VALUES@) [@DEFAULT@]: '
                ),
                prompt=True,
                true=_('Yes'),
                false=_('No'),
                default=False,
            )

        if self.environment[oipcons.RemoveEnv.REMOVE_IMAGEIO_PROXY]:
            self.environment[osetupcons.RemoveEnv.REMOVE_OPTIONS].append(
                oipcons.Const.IMAGEIO_PROXY_PACKAGE_NAME
            )
            self.environment[
                oipcons.ConfigEnv.IMAGEIO_PROXY_STOP_NEEDED
            ] = True

    @plugin.event(
        stage=plugin.Stages.STAGE_MISC,
        condition=lambda self: (
            not self.environment[osetupcons.CoreEnv.DEVELOPER_MODE] and
            not (
                self.environment[osetupcons.RemoveEnv.REMOVE_ALL] or
                self.environment[oipcons.RemoveEnv.REMOVE_IMAGEIO_PROXY]
            )),
    )
    def _misc(self):
        if self.services.exists(
            name=oipcons.Const.IMAGEIO_PROXY_SERVICE_NAME
        ):
            self.services.startup(
                name=oipcons.Const.IMAGEIO_PROXY_SERVICE_NAME,
                state=False,
            )


# vim: expandtab tabstop=4 shiftwidth=4
