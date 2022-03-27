# Releasing ovirt-imageio

## Upstream

1. Add changelog entry with short description of the changes in this
   release:
   https://github.com/oVirt/ovirt-imageio/commit/74bb6d1fe92d4054212859dc9963862effac340b

2. Add new annotated tag e.g "vX.Y.Z"

       $ git tag -a -m "Release X.Y.Z for oVirt 4.5.0" vX.Y.Z

3. Push the tag to github

       $ git push --tags upstream vX.Y.Z

4. Create a new release in github form the new tag:
   https://github.com/oVirt/ovirt-imageio/releases

5. Download the rpms built by github, and upload them to the new
   release:
   https://github.com/oVirt/ovirt-imageio/releases/tag/vX.Y.Z

6. Publish the release.

7. Send a patch to releng-tools project, adding the new version:
   https://github.com/oVirt/releng-tools/commit/97b353b1107ba4bd06776ed4c2e31b309909a357

### Where to get the packages

The packages are available via nsoffer/ovirt-imageio-preview copr repo:
https://copr.fedorainfracloud.org/coprs/nsoffer/ovirt-imageio-preview/

When the releng-tools patch is be merged, the package will be
published in the official repository for the release, via the
ovirt-releaseXY.rpm.

## Pypi

Some oVirt project consume ovirt-imageio via the python package index
(pypi) at:
https://pypi.org/project/ovirt-imageio/

1. Clean the source tree

       $ make clean

2. Enter the ovirt-imageio virtual environment

       $ source ~/venv/ovirt-imageio/bin/activate

3. Install and update pip, build, and twine if needed

       $ pip install --upgrade pip build twine

4. Build the release

       $ python -m build

   This creates:
   - dist/ovirt-imageio-X.Y.Z.tar.gz
   - dist/ovirt_imageio-X.Y.Z-cp310-cp310-linux_x86_64.whl

   We cannot publish the wheel because of binary compatibilities issues.

5. Upload the package to pypi:

       $ python -m twine upload dist/ovirt-imageio-X.Y.Z.tar.gz

   Check the project page to make sure everything looks good:
   https://pypi.org/project/ovirt-imageio/X.Y.Z/

## RHEL

This parts works only inside the Red Hat network.

1. Create source rpm form the release tag

       $ git checkout vX.Y.Z
       $ make srpm

   This source rpm is created in:
   dist/ovirt-imageio-X.Y.Z-1.fc35.src.rpm

2. Login using kinit:

       $ kinit

3. Clone the project with rhpkg if needed:

       $ rhpkg clone ovirt-imageio ovirt-imageio-rhel

4. Import new sources to the right branch.

       $ cd ovirt-imageio-rhel
       $ rhpkg switch-branch rhevm-4.5-rhel-8
       $ rhpkg import ../ovirt-imageio/dist/ovirt-imageio-X.Y.Z-1.el8.src.rpm
       $ rhpkg commit -m 'Release X.Y.Z for RHV 4.5.0'

5. Try a scratch build:

       $ rhpkg scratch-build --srpm

   If the scratch build is successful, you will get mail from brew with
   a scratch repository for testing. Lookup mail with subject like:

       repo for scratch build of ovirt-imageio-2.0.2-2.el8ev is available

   In the mail you will find a repository URL. The repository includes a
   repo file that can be used for testing:

       http://brew-task-repos.usersys.redhat.com/repos/scratch/nsoffer/ovirt-imageio/X.Y.Z/1.el8ev/ovirt-imageio-X.Y.Z-1.el8ev-scratch.repo

6. If the scratch build looks good, push the change and make an official
   build:

       $ rhpkg push
       $ rhpkg build

   Brew will send you an official build repository that can be used for
   testing by other developers or testers. Consider telling people about
   it in rhev-devel mailing list.

## Errata

When the build is ready, you need to add the build to the errata.

Visit https://errata.devel.redhat.com/ and find the errata. To find the
errata, you can do packages search for ovirt-imageio.

To add the build to the errata, usually the easiest way is to use the
package NVR (name, version, release):

    ovirt-imageio-X.Y.Z-1.el8ev

After adding the build, you may get mail about rpmdiff issues. You have
to check the reports and wave the report if needed.

## Post release

Bump version for next development cycle:
https://github.com/oVirt/ovirt-imageio/commit/79f1d6789b833b3acc6fea20264ed57d3a5eab5e
