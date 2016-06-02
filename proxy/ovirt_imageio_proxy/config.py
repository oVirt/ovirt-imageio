
"""
Configuration module.  The values are set in the module namespace so that
IDEs can suggest and validate setting names.  When the module is imported
the first time, the configuration file is read and defaults are overridden.

The type of the default value is used to enforce the type of values loaded
from the config file, which are cast to that type.  Boolean values accept
the strings yes/on/true/1 (ignoring case) as true and are otherwise false.
"""

import logging
import ConfigParser

import constants
import util

_CONFIG_SECTION = 'proxy'

port = 54323
host = ''
use_ssl = True
ssl_key_file = '/path/to/ssl_key_file'
ssl_cert_file = '/path/to/ssl_cert_file'
engine_cert_file = '/path/to/engine_cert_file'
engine_ca_cert_file = '/path/to/engine_ca_cert_file'
verify_certificate = True
poll_interval = 1.0
signed_proxy_ticket = True
allowed_skew_seconds = 0
imaged_connection_timeout_sec = 10
imaged_read_timeout_sec = 30


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
