"""
Version retrieval script.  The build system uses this to read the
VERSION file and retrieve the version and release.  See help/usage
for details.
"""

import argparse
import re
import subprocess
import sys

_regex = re.compile(r"^(\d+\.\d+\.\d+)-(.+)$")

def get_version():
    with open('VERSION') as f:
        v = f.readline().rstrip()
    match = _regex.match(v)
    if not match:
        raise ValueError("Invalid format for version (must be x.y.z-release)")
    version = match.group(1)
    release = match.group(2)

    if release.endswith('.git'):
        try:
            gitrev = subprocess.check_output('git rev-parse --short HEAD'.split())
        except subprocess.CalledProcessError as e:
            raise subprocess.CalledProcessError(
                    "Error getting git revision: " + e)
        release += gitrev.strip()

    return version, release

def main(args):
    parser = argparse.ArgumentParser(
            description='Display Image Upload version')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--full', action='store_true', help='show full version.release')
    group.add_argument('--version', action='store_true', help='show major.minor version')
    group.add_argument('--release', action='store_true', help='show release')
    parsed = parser.parse_args()

    version, release = get_version()
    if parsed.version:
        print version
    elif parsed.release:
        print release
    else:
        print '{version}-{release}'.format(**locals())

if __name__ == '__main__':
    sys.exit(main(sys.argv))
