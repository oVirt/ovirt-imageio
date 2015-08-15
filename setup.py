
from distutils.core import setup

import version as version

full_version = '.'.join(version.get_version())

setup(
    #package_dir = {'ovirt_image_proxy': 'image-proxy'},
    name='ovirt_image_proxy',
    version=full_version,
    description='oVirt Image Upload Proxy',
    license='ASL 2.0',
    url='http://wiki.ovirt.org/Features/Image_Upload',
    author='Greg Padgett',
    author_email='gpadgett@redhat.com',
    packages=['ovirt_image_proxy'],
    scripts=['ovirt-image-proxy'],
    # TODO min versions?
    requires=['requests', 'webob'],
)
