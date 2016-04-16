
from distutils.core import setup

import version as version

full_version = '.'.join(version.get_version())

setup(
    author="Nir Soffer, Greg Padgett, Amit Aviram",
    author_email="nsoffer@redhat.com, gpadgett@redhat.com, aaviram@redhat.com",
    description='oVirt imageio proxy',
    license="GNU GPLv2+",
    name='ovirt-imageio-proxy',
    packages=['ovirt_image_proxy'],
    requires=['requests', 'webob'],  # versions?
    scripts=['ovirt-image-proxy'],
    url="https://gerrit.ovirt.org/ovirt-imageio",
    version=full_version,
)
