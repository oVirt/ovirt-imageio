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
from ovirt_setup_lib import hostname as osetuphostname
from ovirt_setup_lib import dialog
from ovirt_engine_setup.engine import constants as oenginecons
from ovirt_engine_setup.engine import vdcoption
from ovirt_engine_setup.engine_common import constants as oengcommcons
from ovirt_engine_setup.ovirt_imageio_proxy import constants as oipcons


def _(m):
    return gettext.dgettext(message=m, domain='ovirt-imageio-proxy-setup')


@util.export
class Plugin(plugin.PluginBase):
    """ovirt-imageio-proxy plugin."""

    def __init__(self, context):
        super(Plugin, self).__init__(context=context)
        self._needStart = False
        self._enabled = True

    @plugin.event(
        stage=plugin.Stages.STAGE_INIT,
    )
    def _init(self):
        self.environment.setdefault(
            oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG,
            None
        )
        self.environment.setdefault(
            oipcons.ConfigEnv.IMAGEIO_PROXY_PORT,
            oipcons.ConfigEnv.DEFAULT_IMAGEIO_PROXY_PORT
        )
        self.environment.setdefault(
            oipcons.ConfigEnv.IMAGEIO_PROXY_HOST,
            'localhost'
        )
        self.environment.setdefault(
            oipcons.EngineConfigEnv.ENGINE_FQDN,
            None
        )
        self.environment.setdefault(
            oipcons.EngineCoreEnv.ENABLE,
            None
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_LATE_SETUP,
        condition=lambda self: not self.environment[
            osetupcons.CoreEnv.DEVELOPER_MODE
        ],
    )
    def _late_setup_service_state(self):
        self._needStart = self.services.status(
            name=oipcons.Const.IMAGEIO_PROXY_SERVICE_NAME,
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_CUSTOMIZATION,
        name=oipcons.Stages.CONFIG_IMAGEIO_PROXY_CUSTOMIZATION,
        condition=lambda self: self._enabled,
        before=(
            osetupcons.Stages.DIALOG_TITLES_E_PRODUCT_OPTIONS,
        ),
        after=(
            osetupcons.Stages.DIALOG_TITLES_S_PRODUCT_OPTIONS,
        ),
    )
    def _customization(self):

        if self.environment[
            oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG
        ] is None:
            self.environment[
                oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG
            ] = dialog.queryBoolean(
                dialog=self.dialog,
                name='OVESETUP_CONFIG_IMAGEIO_PROXY',
                note=_(
                    'Configure Image I/O Proxy on this host? '
                    '(@VALUES@) [@DEFAULT@]: '
                ),
                prompt=True,
                default=True,
            )
        self._enabled = self.environment[
            oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG
        ]
        if self._enabled:
            self.environment[
                oipcons.ConfigEnv.IMAGEIO_PROXY_STOP_NEEDED
            ] = True

    @plugin.event(
        stage=plugin.Stages.STAGE_CUSTOMIZATION,
        after=(
            osetupcons.Stages.DIALOG_TITLES_S_NETWORK,
            oengcommcons.Stages.NETWORK_OWNERS_CONFIG_CUSTOMIZED,
        ),
        before=(
            osetupcons.Stages.DIALOG_TITLES_E_NETWORK,
        ),
        condition=lambda self: self.environment[
            oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG
        ],
    )
    def _customization_network(self):
        osetuphostname.Hostname(
            plugin=self,
        ).getHostname(
            envkey=oipcons.EngineConfigEnv.ENGINE_FQDN,
            whichhost=_('the engine'),
            supply_default=False,
            validate_syntax=True,
            system=True,
            dns=False,
            local_non_loopback=self.environment[
                osetupcons.ConfigEnv.FQDN_NON_LOOPBACK_VALIDATION
            ],
            reverse_dns=self.environment[
                osetupcons.ConfigEnv.FQDN_REVERSE_VALIDATION
            ],
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_CUSTOMIZATION,
        condition=lambda self: self.environment[
            oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG
        ],
        before=(
            osetupcons.Stages.DIALOG_TITLES_E_SYSTEM,
        ),
        after=(
            osetupcons.Stages.DIALOG_TITLES_S_SYSTEM,
            oipcons.Stages.CONFIG_IMAGEIO_PROXY_CUSTOMIZATION,
        ),
    )
    def _customization_firewall(self):
        self.environment[osetupcons.NetEnv.FIREWALLD_SERVICES].extend([
            {
                'name': 'ovirt-imageio-proxy',
                'directory': 'ovirt-imageio-proxy'
            },
        ])
        self.environment[
            osetupcons.NetEnv.FIREWALLD_SUBST
        ].update({
            '@IMAGEIO_PROXY_PORT@': self.environment[
                oipcons.ConfigEnv.IMAGEIO_PROXY_PORT
            ],
        })

    @plugin.event(
        stage=plugin.Stages.STAGE_MISC,
        condition=lambda self: self._enabled,
        after=(
            oipcons.Stages.CA_AVAILABLE,
        ),
    )
    def _check_separate(self):
        self.logger.info(_('Configuring Image I/O Proxy'))
        if (
            not os.path.exists(
                oipcons.FileLocations.
                OVIRT_ENGINE_PKI_ENGINE_CERT
            )
        ):
            self.dialog.note(
                text=_(
                    "\n"
                    "ATTENTION\n"
                    "\n"
                    "Manual actions are required on "
                    "the engine host in order to\n"
                    "enroll certs for this host and "
                    "configure the engine to use it.\n"
                )
            )

    @plugin.event(
        stage=plugin.Stages.STAGE_MISC,
        name=oipcons.Stages.REMOTE_VDC,
        condition=lambda self: (
            self._enabled and
            not os.path.exists(
                oipcons.FileLocations.
                OVIRT_ENGINE_PKI_ENGINE_CERT
            )
        ),
        after=(
            oipcons.Stages.CA_AVAILABLE,
        ),
    )
    def _misc_VDC(self):
        self.dialog.note(
            text=_(
                "\nPlease execute this command on the engine host:\n"
                "   engine-config -s ImageProxyAddress={fqdn}:{port}\n"
                "and then restart the engine service to make it effective\n\n"
            ).format(
                fqdn=self.environment[osetupcons.ConfigEnv.FQDN],
                port=self.environment[
                    oipcons.ConfigEnv.IMAGEIO_PROXY_PORT
                ],
            ),
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_MISC,
        condition=lambda self: (
            self._enabled,
        ),
    )
    def _misc_config(self):
        self.environment[otopicons.CoreEnv.MAIN_TRANSACTION].append(
            filetransaction.FileTransaction(
                name=oipcons.FileLocations.OVIRT_IMAGEIO_PROXY_CONFIG,
                content=self._get_configuration(),
                modifiedList=self.environment[otopicons.CoreEnv.MODIFIED_FILES]
            )
        )

    def _get_configuration(self):
        return textwrap.dedent(
            """\
            [proxy]
            # Listening port
            port = {port}

            # Listening addresses (empty for all)
            host =

            # Wrap incoming connections with SSL
            use_ssl = true

            # Key file for SSL connections
            ssl_key_file = {key}

            # Certificate file for SSL connections
            ssl_cert_file = {cert}

            # Certificate file used when decoding signed token
            engine_cert_file = {engine_cert}

            # CA certificate file used to verify signed token
            engine_ca_cert_file = {ca_cert}

            # Verify the certificate used to decode the signed token
            verify_certificate = true

            # Server shutdown request polling interval, in seconds
            # poll_interval = 1.0

            # Signed proxy ticket; false for plain-text JSON
            # signed_proxy_ticket = true

            # Allowed time drift between signed ticket issuer and proxy
            # host, considered when checking ticket validity
            # allowed_skew_seconds = 0

            # Seconds to wait while connecting to the ovirt-imageio-daemon
            # imaged_connection_timeout_sec = 10

            # Seconds to wait while reading from the ovirt-imageio-daemon
            # imaged_read_timeout_sec = 30
            """
        ).format(
            port=self.environment[oipcons.ConfigEnv.IMAGEIO_PROXY_PORT],
            key=oipcons.FileLocations.OVIRT_ENGINE_PKI_IMAGEIO_PROXY_KEY,
            cert=oipcons.FileLocations.OVIRT_ENGINE_PKI_IMAGEIO_PROXY_CERT,
            engine_cert=oipcons.FileLocations.OVIRT_ENGINE_PKI_ENGINE_CERT,
            ca_cert=oipcons.FileLocations.OVIRT_ENGINE_PKI_ENGINE_CA_CERT,
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_MISC,
        after=(
                oengcommcons.Stages.DB_CONNECTION_AVAILABLE,
        ),
        condition=lambda self: (
                self.environment[
                    oenginecons.CoreEnv.ENABLE
                ] and self.environment[
                    oenginecons.EngineDBEnv.NEW_DATABASE
                ]
        ),
    )
    def _databaseOptions(self):
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
                        oipcons.ConfigEnv.DEFAULT_IMAGEIO_PROXY_PORT,
                    ),
                },
            ),
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_CLOSEUP,
        condition=lambda self: (
            not self.environment[
                osetupcons.CoreEnv.DEVELOPER_MODE
            ] and (
                self._needStart or
                self._enabled
            )
        ),
    )
    def _closeup(self):
        for state in (False, True):
            self.services.state(
                name=oipcons.Const.IMAGEIO_PROXY_SERVICE_NAME,
                state=state,
            )
        self.services.startup(
            name=oipcons.Const.IMAGEIO_PROXY_SERVICE_NAME,
            state=True,
        )


# vim: expandtab tabstop=4 shiftwidth=4
