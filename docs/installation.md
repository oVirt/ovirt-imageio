# Installation

## Standalone installation

Starting version 2.0, imageio supports only Python 3 and Python 2 is
not supported any more.  To install imageio service, run:

    sudo dnf install ovirt-imageio-daemon


## Installation with ovirt-engine

To install imageio in ovirt-engine environment, run:

    engine-setup

This will install ovirt-imageio-daemon itself and all necessary
dependencies and launch the setup, which is a module run by the oVirt
engine setup program.


### Installation with ovirt-engine developer environment

Please see [imageio
section](https://github.com/ovirt/ovirt-engine#ovirt-imageio) in oVirt
[README](https://github.com/oVirt/ovirt-engine/blob/master/README.adoc)
how to configure imageio in ovirt-engine developer environment.


## Configuration

After installation, you must confgiure TLS and optionally you may want
to change default configuration. See [configuration](configuration.md)
page how to configure imageio and what are available configuration
options.