# Releasing ovirt-imageio


## Upstream

- Bump version for release and merge the patch:
  https://gerrit.ovirt.org/108091/

- Add changelog entry with short description of the changes in this
  release.

- Add new annotated tag e.g "v2.0.2"

    $ git tag -a -m "Release 2.0.1 for oVirt 4.4.0" v2.0.2

- Push the tag to gerrit

    $ git push --tags origin v2.0.2

- Send a patch to releng-tools project, adding the new version:
  See https://gerrit.ovirt.org/108073/


### Where to get the packages

This release tarball will be available immediately in github at:
https://github.com/oVirt/ovirt-imageio/releases

The rpm will be available in few minutes in the jenkins build for the
version patch. Look for the build comment like:
https://gerrit.ovirt.org/c/108091/#message-b7b37559_79ba9e75

Follow the link to the build, and follow the "Built artifacts link":
https://jenkins.ovirt.org/job/ovirt-imageio_standard-on-merge/555/artifact/build-artifacts.py3.el8.x86_64/

You can add a yum repo with this URL as base_url to upgrade
ovirt-imageio.

The packages will be available later (few hours or days) at ovirt
repositories:
https://resources.ovirt.org/pub/ovirt-master-snapshot/rpm/el8/

When the releng-tools patch is be merged, the package will be
published in the official repository for the release:
https://resources.ovirt.org/pub/ovirt-4.4-pre/rpm/el8/x86_64/


## RHEL

This parts works only inside the Red Hat network.

Login using kinit:

   $ kinit

Clone the project with rhpkg if needed:

    $ rhpkg clone ovirt-imageio ovirt-imageio-rhel

Import new sources to the right branch.

    $ cd ovirt-imageio-rhel
    $ rhpkg switch-branch rhevm-4.4-rhel-8
    $ wget https://jenkins.ovirt.org/job/ovirt-imageio_standard-on-merge/555/artifact/build-artifacts.py3.el8.x86_64/ovirt-imageio-2.0.2-0.el8.src.rpm
    $ rhpkg import ovirt-imageio-2.0.2-0.el8.src.rpm
    $ rhpkg commit -m 'Release 2.0.2 for RHV 4.4.0'

Try a scratch build:

    $ rhpkg scratch-build --srpm

If the scratch build is successful, you will get mail from brew with a
scratch repository for testing. Lookup mail with subject like:

    "repo for scratch build of ovirt-imageio-2.0.2-2.el8ev is available"

In the mail you will find a repository URL. The repository includes a
repo file that can be used for testing:

    http://brew-task-repos.usersys.redhat.com/repos/scratch/nsoffer/ovirt-imageio/2.0.2/2.el8ev/ovirt-imageio-2.0.2-2.el8ev-scratch.repo

If the scratch build looks good, push the change and make an official
build:

    $ rhpkg push
    $ rhpkg build

Brew will send you an official build repository that can be used for
testing by other developers or testers. Consider telling people about it
in rhev-devel mailing list.


## Errata

When the build is ready, you need to add the build to the errata.

Visit https://errata.devel.redhat.com/ and find the errata. To find the
errata, you can do packages search for ovirt-imageio, or vdsm.

To add the build to the errata, usually the easiest way is to use the
package NVR (name, version, release):

    ovirt-imageio-2.0.2-2.el8ev

After adding the build, you may get mail about rpmdiff issues. You have
to check the reports and wave the report if needed.
