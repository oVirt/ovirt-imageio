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


"""ovirt-imageio-proxy constants."""


import gettext
import os

from otopi import util

from ovirt_engine_setup.constants import osetupattrs
from ovirt_engine_setup.constants import osetupattrsclass

from . import config as oipconfig


def _(m):
    return gettext.dgettext(message=m, domain='ovirt-imageio-proxy-setup')


@util.export
class Const(object):
    IMAGEIO_PROXY_SERVICE_NAME = 'ovirt-imageio-proxy'
    IMAGEIO_PROXY_PACKAGE_NAME = 'ovirt-imageio-proxy'
    IMAGEIO_PROXY_SETUP_PACKAGE_NAME = \
        'ovirt-imageio-proxy-setup'
    IMAGEIO_PROXY_CERT_NAME = 'imageio-proxy'


@util.export
class FileLocations(object):

    OVIRT_IMAGEIO_PROXY_CONFIG = \
        oipconfig.OVIRT_IMAGEIO_PROXY_CONFIG

    OVIRT_ENGINE_PKIDIR = oipconfig.ENGINE_PKIDIR

    OVIRT_ENGINE_PKIKEYSDIR = os.path.join(
        OVIRT_ENGINE_PKIDIR,
        'keys',
    )
    OVIRT_ENGINE_PKICERTSDIR = os.path.join(
        OVIRT_ENGINE_PKIDIR,
        'certs',
    )
    OVIRT_ENGINE_PKIREQUESTSDIR = os.path.join(
        OVIRT_ENGINE_PKIDIR,
        'requests',
    )

    OVIRT_ENGINE_PKI_IMAGEIO_PROXY_KEY = os.path.join(
        OVIRT_ENGINE_PKIKEYSDIR,
        '%s.key.nopass' % Const.IMAGEIO_PROXY_CERT_NAME,
    )
    OVIRT_ENGINE_PKI_IMAGEIO_PROXY_CERT = os.path.join(
        OVIRT_ENGINE_PKICERTSDIR,
        '%s.cer' % Const.IMAGEIO_PROXY_CERT_NAME,
    )

    OVIRT_ENGINE_PKI_IMAGEIO_PROXY_REQ = os.path.join(
        OVIRT_ENGINE_PKICERTSDIR,
        '%s.req' % Const.IMAGEIO_PROXY_CERT_NAME,
    )
    OVIRT_ENGINE_PKI_ENGINE_CERT = os.path.join(
        OVIRT_ENGINE_PKICERTSDIR,
        'engine.cer',
    )


@util.export
class Stages(object):

    CA_AVAILABLE = 'osetup.pki.ca.available'

    CONFIG_IMAGEIO_PROXY_CUSTOMIZATION = \
        'setup.config.imageio-proxy.customization'

    REMOTE_VDC = 'setup.config.imageio-proxy.remote_vdc'

    # sync with engine
    ENGINE_CORE_ENABLE = 'osetup.engine.core.enable'


@util.export
class Defaults(object):
    DEFAULT_KEY_SIZE = 2048


@util.export
@util.codegen
@osetupattrsclass
class ConfigEnv(object):

    IMAGEIO_PROXY_HOST = 'OVESETUP_CONFIG/imageioProxyHost'

    IMAGEIO_PROXY_PORT = 'OVESETUP_CONFIG/imageioProxyPort'

    DEFAULT_IMAGEIO_PROXY_PORT = 54323

    @osetupattrs(
        answerfile=True,
        summary=True,
        description=_('Configure Image I/O Proxy'),
        postinstallfile=True,
    )
    def IMAGEIO_PROXY_CONFIG(self):
        return 'OVESETUP_CONFIG/imageioProxyConfig'

    CERTIFICATE_ENROLLMENT = 'OVESETUP_CONFIG/certificateEnrollment'

    KEY_SIZE = 'OVESETUP_CONFIG/keySize'

    REMOTE_ENGINE_HOST = 'OVESETUP_CONFIG/remoteEngineHost'

    OIP_CERTIFICATE_CHAIN = 'OVESETUP_CONFIG/oipCertificateChain'
    REMOTE_ENGINE_CER = 'OVESETUP_CONFIG/remoteEngineCer'

    PKI_OIP_CSR_FILENAME = 'OVESETUP_CONFIG/pkiOIPCSRFilename'

    IMAGEIO_PROXY_STOP_NEEDED = 'OVESETUP_CONFIG/imageioProxyStopNeeded'


@util.export
@util.codegen
@osetupattrsclass
class EngineCoreEnv(object):
    """Sync with ovirt-engine"""
    ENABLE = 'OVESETUP_ENGINE_CORE/enable'


@util.export
@util.codegen
@osetupattrsclass
class RemoveEnv(object):
    @osetupattrs(
        answerfile=True,
    )
    def REMOVE_IMAGEIO_PROXY(self):
        return 'OVESETUP_REMOVE/removeOip'


@util.export
@util.codegen
@osetupattrsclass
class RPMDistroEnv(object):
    PACKAGES = 'OVESETUP_OIP_RPMDISRO_PACKAGES'
    PACKAGES_SETUP = 'OVESETUP_OIP_RPMDISRO_PACKAGES_SETUP'


@util.export
@util.codegen
class Displays(object):
    CERTIFICATE_REQUEST = 'OIP_CERTIFICATE_REQUEST'


@util.export
@util.codegen
@osetupattrsclass
class EngineConfigEnv(object):
    """Sync with ovirt-engine"""

    @osetupattrs(
        answerfile=True,
        summary=True,
        description=_('Engine Host FQDN'),
        postinstallfile=True,
    )
    def ENGINE_FQDN(self):
        return 'OVESETUP_ENGINE_CONFIG/fqdn'


# vim: expandtab tabstop=4 shiftwidth=4
