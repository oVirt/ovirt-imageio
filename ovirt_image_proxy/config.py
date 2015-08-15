
"""
Configuration module.  The values are set in the module namespace so that
IDEs can suggest and validate setting names.  When the module is imported
the first time, the configuration file is read and defaults are overridden.

The type of the default value is used to enforce the type of values loaded
from the config file, which are cast to that type.  Boolean values accept
the strings yes/on/true/1 (ignoring case) as true and are otherwise false.
"""

import logging
import os
import ConfigParser

import constants
import util

_CONFIG_SECTION = 'configuration'


# TODO constants.py for pathnames

engine_cert = '/tmp/engine-cert.pem'
ca_cert = ''
signing_cert = '~/ovirt-engine/etc/pki/ovirt-engine/certs/ca.der'
signing_key = '~/ovirt-engine/etc/pki/ovirt-engine/private/ca.pem'

pki_dir = '/etc/pki/vdsm'
host = ""
port = 8081
poll_interval = 1.0
buffer_size = 64 * 1024
run_loop_seconds = 1.0
verify_certificate = False
json_proxy_token = False
use_ssl = False
imaged_ssl = True
imaged_port = 54322
allowed_skew_seconds = 0

imaged_connection_timeout_sec = 10
imaged_read_timeout_sec = 30


# Derived configuration values
key_file = None
def _set_key_file():
    global key_file
    key_file = os.path.join(pki_dir, 'keys', 'vdsmkey.pem')

cert_file = None
def _set_cert_file():
    global cert_file
    cert_file = os.path.join(pki_dir, 'keys', 'vdsmcert.pem')


def _set(name, value):
    if name.startswith('_') or name not in globals():
        raise ValueError("Invalid configuration value '{}'".format(name))
    t = type(globals()[name])
    if t in (callable, property):
        raise ValueError('Cannot set derived configuration value')

    try:
        if t == bool:
            globals()[name] = util.to_bool(value)
        else:
            globals()[name] = t(value)
    except ValueError:
        raise ValueError("Invalid type for configuration value '{}', must be: {}".format(name, t))

    logging.debug("Configuration value {} = {}".format(name, value))


def load(config_file=None):
    config = ConfigParser.ConfigParser()
    try:
        config.read(config_file if config_file else constants.CONFIG_FILE)

        # First set values from config file...
        for name, value in config.items(_CONFIG_SECTION):
            _set(name, value)

    except ConfigParser.Error as e:
        e.message = (
                "Error reading config file {}: {}"
                .format(constants.CONFIG_FILE, e.message)
        )
        raise

    # ... then set the derived values
    for name in globals():
        if name.startswith('_set_'):
            globals()[name]()
