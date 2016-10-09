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


import contextlib
import gettext
import os
import time

from six.moves.urllib.request import urlopen

from otopi import constants as otopicons
from otopi import filetransaction
from otopi import plugin
from otopi import util

from ovirt_engine_setup import constants as osetupcons
from ovirt_engine_setup import remote_engine
from ovirt_engine_setup.engine import constants as oenginecons
from ovirt_engine_setup.engine_common import constants as oengcommcons
from ovirt_engine_setup.ovirt_imageio_proxy import constants as oipcons


def _(m):
    return gettext.dgettext(message=m, domain='ovirt-imageio-proxy-setup')


@util.export
class Plugin(plugin.PluginBase):
    """ovirt-imageio-proxy plugin."""

    def __init__(self, context):
        super(Plugin, self).__init__(context=context)
        self._enabled = False
        self._enrollment_data = None
        self._need_eng_cert = False
        self._engine_cert = None

    @plugin.event(
        stage=plugin.Stages.STAGE_INIT,
    )
    def _init(self):
        self.environment.setdefault(
            oipcons.ConfigEnv.PKI_OIP_CSR_FILENAME,
            None
        )

    @plugin.event(
        stage=plugin.Stages.STAGE_CUSTOMIZATION,
        before=(
            oengcommcons.Stages.DIALOG_TITLES_E_PKI,
        ),
        after=(
            oipcons.Stages.CONFIG_IMAGEIO_PROXY_CUSTOMIZATION,
            oenginecons.Stages.CORE_ENABLE,
            oengcommcons.Stages.DIALOG_TITLES_S_PKI,
        ),
        condition=lambda self: (
            self.environment[
                oipcons.ConfigEnv.IMAGEIO_PROXY_CONFIG
            ] and
            # If on same host as engine, engine setup code creates pki for us
            not self.environment[
                oenginecons.CoreEnv.ENABLE
            ]
        ),
    )
    def _customization(self):
        self._enabled = True

        engine_oip_pki_found = (
            os.path.exists(
                oipcons.FileLocations.OVIRT_ENGINE_PKI_IMAGEIO_PROXY_KEY
            ) and os.path.exists(
                oipcons.FileLocations.OVIRT_ENGINE_PKI_IMAGEIO_PROXY_CERT
            ) and os.path.exists(
                oipcons.FileLocations.OVIRT_ENGINE_PKI_ENGINE_CERT
            )
        )

        if not engine_oip_pki_found:
            self._enrollment_data = remote_engine.EnrollCert(
                remote_engine=self.environment[
                    osetupcons.CoreEnv.REMOTE_ENGINE
                ],
                engine_fqdn=self.environment[
                    oenginecons.ConfigEnv.ENGINE_FQDN
                ],
                base_name=oipcons.Const.IMAGEIO_PROXY_CERT_NAME,
                base_touser=_('Image I/O Proxy'),
                key_file=oipcons.FileLocations.
                OVIRT_ENGINE_PKI_IMAGEIO_PROXY_KEY,
                cert_file=oipcons.FileLocations.
                OVIRT_ENGINE_PKI_IMAGEIO_PROXY_CERT,
                csr_fname_envkey=oipcons.ConfigEnv.
                PKI_OIP_CSR_FILENAME,
                engine_ca_cert_file=os.path.join(
                    oipcons.FileLocations.OVIRT_ENGINE_PKIDIR,
                    'ca.pem'
                ),
                engine_pki_requests_dir=oipcons.FileLocations.
                OVIRT_ENGINE_PKIREQUESTSDIR,
                engine_pki_certs_dir=oipcons.FileLocations.
                OVIRT_ENGINE_PKICERTSDIR,
                key_size=oipcons.Defaults.DEFAULT_KEY_SIZE,
                url="http://www.ovirt.org/develop/release-management"
                    "/features/storage/image-upload/",
            )
            self._enrollment_data.enroll_cert()

            self._need_eng_cert = not os.path.exists(
                oipcons.FileLocations.
                OVIRT_ENGINE_PKI_ENGINE_CERT
            )
        else:
            self._enabled = False

        tries_left = 30
        while (
            self._need_eng_cert and
            self._engine_cert is None and
            tries_left > 0
        ):
            remote_engine_host = self.environment[
                oenginecons.ConfigEnv.ENGINE_FQDN
            ]

            # TODO format=X509-PEM-CA ?
            with contextlib.closing(
                urlopen(
                    'http://{engine_fqdn}/ovirt-engine/services/'
                    'pki-resource?resource=engine-certificate&'
                    'format=X509-PEM'.format(
                        engine_fqdn=remote_engine_host
                    )
                )
            ) as urlObj:
                engine_ca_cert = urlObj.read()
                if engine_ca_cert:
                    self._engine_cert = engine_ca_cert
                else:
                    self.logger.error(
                        _(
                            'Failed to get the engine certificate '
                            'from the engine host. '
                            'Please check access to the engine and its '
                            'status.'
                        )
                    )
                    time.sleep(10)
                    tries_left -= 1
        if self._need_eng_cert and self._engine_cert is None:
            raise RuntimeError(_('Failed to get the engine certificate from '
                                 'the engine host'))

    @plugin.event(
        stage=plugin.Stages.STAGE_MISC,
        condition=lambda self: (
            self._enabled
        ),
        after=(
            oipcons.Stages.CA_AVAILABLE,
        ),
    )
    def _misc_pki(self):
        self._enrollment_data.add_to_transaction(
            uninstall_group_name='ca_pki_oip',
            uninstall_group_desc='OIP PKI keys',
        )
        uninstall_files = []
        self.environment[
            osetupcons.CoreEnv.REGISTER_UNINSTALL_GROUPS
        ].createGroup(
            group='ca_pki_oip',
            description='OIP PKI keys',
            optional=True,
        ).addFiles(
            group='ca_pki_oip',
            fileList=uninstall_files,
        )

        if self._need_eng_cert:
            self.environment[otopicons.CoreEnv.MAIN_TRANSACTION].append(
                filetransaction.FileTransaction(
                    name=oipcons.FileLocations.
                    OVIRT_ENGINE_PKI_ENGINE_CERT,
                    mode=0o600,
                    owner=self.environment[
                        osetupcons.SystemEnv.USER_ENGINE
                    ],
                    enforcePermissions=True,
                    content=self._engine_cert,
                    modifiedList=uninstall_files,
                )
            )
            uninstall_files.append(
                oipcons.FileLocations.OVIRT_ENGINE_PKI_ENGINE_CERT
            )

    @plugin.event(
        stage=plugin.Stages.STAGE_CLEANUP,
        condition=lambda self: (
            self._enabled
        ),
    )
    def _cleanup(self):
        self._enrollment_data.cleanup()


# vim: expandtab tabstop=4 shiftwidth=4
