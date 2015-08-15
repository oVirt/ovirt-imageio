#!/bin/sh -x

#./ovirt-image-proxy --log_config_file=conf/logger.conf --config_file=conf/config.ini --stdout

./ovirt-image-proxy --log_config_file=conf/logger.conf --config_file=_config_overrides.ini --stdout=debug #--pydevd localhost:22200
