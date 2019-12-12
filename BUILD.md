# Build ovirt-imageio

## Upstream

- Update version.py in all packages
- Add new annotated tag e.g "v1.6.3"
- Push tag to gerrit
- Merge the version patch; CI will build the packages and push them to
  resources.ovirt.org.

## RHEL

This parts works only inside the Red Hat network.

Login using kinit:

   $ kinit

Clone 3 projects with rhpkg if needed:

    $ for proj in ovirt-imageio-common ovirt-imageio-daemon ovirt-imageio-proxy; do
        rhpkg clone $proj
    done

For every package, import new sources to the right branch.

### ovirt-imageio-common

Currently building for both rhevm-4.4-rhel-7 (python 2) and
rhevm-4.4-rhel-8 (python 3), so we need to import the sources twice.

For RHEL 7:

    $ cd ovirt-imageio-common
    $ rhpkg switch-branch rhevm-4.4-rhel-7
    $ wget https://jenkins.ovirt.org/job/ovirt-imageio_standard-check-patch/1898/artifact/build-artifacts.py2.el7.x86_64/ovirt-imageio-common-1.6.2-0.el7.src.rpm
    $ rhpkg import ovirt-imageio-common-1.6.2-0.el7.src.rpm
    $ rhpkg commit -m 'Release 1.6.2 for RHV 4.4'
    $ rhpkg scratch-build --srpm

If the scratch build was successful, push the changes and create a
build:

    $ rhpkg push
    $ rhpkg build

Now do the same for RHEL 8:

    $ rhpkg switch-branch rhevm-4.4-rhel-8
    $ wget https://jenkins.ovirt.org/job/ovirt-imageio_standard-check-patch/1898/artifact/build-artifacts.py3.el8.x86_64/ovirt-imageio-common-1.6.2-0.el8.src.rpm
    $ rhpkg import ovirt-imageio-common-1.6.2-0.el8.src.rpm
    $ rhpkg commit -m 'Release 1.6.2 for RHV 4.4'
    $ rhpkg scratch-build --srpm
    $ rhpkg push
    $ rhpkg build

### ovirt-imageio-daemon

We build the daemon only for RHEL 8:

    $ cd ovirt-imageio-daemon
    $ rhpkg switch-branch rhevm-4.4-rhel-8
    $ wget https://jenkins.ovirt.org/job/ovirt-imageio_standard-check-patch/1898/artifact/build-artifacts.py3.el8.x86_64/ovirt-imageio-daemon-1.6.2-0.el8.src.rpm
    $ rhpkg import ovirt-imageio-daemon-1.6.2-0.el8.src.rpm
    $ rhpkg commit -m 'Release 1.6.2 for RHV 4.4'
    $ rhpkg scratch-build --srpm
    $ rhpkg push
    $ rhpkg build

### ovirt-imageio-proxy

We build the daemon only for RHEL 7:

    $ cd ovirt-imageio-proxy
    $ rhpkg switch-branch rhevm-4.4-rhel-7
    $ wget https://jenkins.ovirt.org/job/ovirt-imageio_standard-check-patch/1898/artifact/build-artifacts.py2.el7.x86_64/ovirt-imageio-proxy-1.6.2-0.el7.src.rpm
    $ rhpkg import ovirt-imageio-proxy-1.6.2-0.el7.src.rpm
    $ rhpkg commit -m 'Release 1.6.2 for RHV 4.4'
    $ rhpkg scratch-build --srpm
    $ rhpkg push
    $ rhpkg build

## Errata

When the builds are ready, you need to add the builds to the errata.

Visit https://errata.devel.redhat.com/, find the errata, and add the
builds.

After adding the builds, you may get mail about rpmdiff issues. You have
to check the reports and wave the report if needed.
