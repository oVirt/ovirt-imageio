# Installation

## ovirt-imageio-proxy

The image proxy can be installed on most hosts that have a working
Python 2 environment with the needed dependencies.  To install the
rpm package:

  sudo yum install ovirt-imageio-proxy
  engine-setup

This will install all necessary dependencies and launch the setup,
which is a module run by the oVirt engine setup program.  The prompts
during setup depend on other installed packages; if ovirt-engine is
on the host you may be prompted with additional questions.  On a
separate host, you will be prompted to perform manual steps for the
PKI setup.

Installation from source is not recommended.  If you want to run the
proxy without installing the packages and running engine-setup, it's
best to run it in-place using the --config_file option to supply the
runtime configuration.


### Installation with ovirt-engine developer environment

For setting up ovirt-imageio-proxy in an ovirt dev environment, run:

   make install-dev ENGINE_PREFIX=<engine prefix>

Example:

   make install-dev ENGINE_PREFIX=/home/user/my-ovirt-engine

If ovirt-imageio-proxy wasn't configured yet, run engine setup, and
choose "Yes" when prompted to install oVirt ImageIO proxy. After engine
setup finishes to run, run `ovirt-imageio-proxy`.


### SSL installation

For using imageIO via the webadmin, the client will need to install
oVirt's CA in its browser. oVirt's CA certificate can be fetched from
the following link, as specified in oVirt's PKI wiki page
http://www.ovirt.org/develop/release-management/features/infra/pki/:

http://<engine_url>/ovirt-engine/services/pki-resource?resource=ca-certificate&format=X509-PEM-CA
