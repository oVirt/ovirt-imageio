# Configuration

## TLS Configuration

In order to secure bits sent over the network, ovirt-imageio encrypts
the communication using TLS.  Using TLS is by default turned
on. Running imageio as a proxy without TLS enabled is not implemented.
The keys paths can be configured in `[tls]` section of a drop-in
config file `*.conf` placed in `/etc/ovirt-imageio/conf.d` or
`/usr/lib/ovirt-imageio/conf.d`.  Imageio does not provide any default
TLS certificates and these has to be configured by the user.


### Configuration options

- `enable`: Enables TLS. By default, TLS is enabled.  This option is
  only for development purposes and in production should be always
  turned on, otherwise other parties that can monitor network traffic
  to discover ticket ids and gain access to all active image
  transfers. When imageio is used as a proxy, this has to be tuned on,
  as HTTP proxy is not implemented.


- `key_file`: The private key of the ovirt-imageio service which is
  used to implement the SSL server.


- `cert_file`: The server certificate of ovirt-imageio service which is
  also used to implement the SSL server.


- `ca_file`: File with trusted certificates. This should be used when
  `cert_file` is not signed by a certificate authority trusted by the
  host. Leave empty if `cert_file` is signed by a trusted certificate
  authority.


- `enable_tls1_1`: Enables TLSv1.1, for legacy user applications that
  do not support TLSv1.2.


### TLS configuration on oVirt engine host

TLS is used to communicate securly with clients using oVirt image
transfer API, or with oVit engine Administration Portal.
Configuration used by imageio on oVirt engine host is placed in
`/etc/ovirt-imageio/conf.d/50-engine.conf` and is configured by oVirt
`engine-setup`.  This file is owned by oVirt engine and any custom
changes to imageio configuration should be placed into dedicated
`*.conf` file with higher name (ordered alphabetically) in `conf.d`
direcotry, e.g. `/etc/ovirt-imageio/conf.d/99-user.conf`.


### Browser configuration

For using imageio via the oVirt engine Adminsitration Portal, the
client will need to install oVirt's CA in its browser. oVirt's CA
certificate can be fetched from the following link, as specified in
[oVirt's PKI wiki page](http://www.ovirt.org/develop/release-management/features/infra/pki/):

    'http://{engine_url}/ovirt-engine/services/pki-resource?resource=ca-certificate&format=X509-PEM-CA'

e.g.

    curl -k
    'https://ovirt-imageio.local/ovirt-engine/services/pki-resource?resource=ca-certificate&format=X509-PEM-CA'
    > cert.pem


### TLS configuration on oVirt host

TLS is used in order to securely communicate with clients using oVirt
image transfer API.  imageio is configured by vdsm and the
configuration file is place in
`/etc/ovirt-imageio/conf.d/50-vdsm.conf`. This file is owned by vdsm
and any custom changes to imageio configuration should be placed into
dedicated `*.conf` file with higher name (ordered alphabetically) in
`conf.d` direcotry, e.g. `/etc/ovirt-imageio/conf.d/99-user.conf`.