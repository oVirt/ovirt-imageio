#!/bin/bash -e

# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

create_loop_devices() {
    local last=$(($1-1))
    local min
    for min in `seq 0 $last`; do
        local name=/dev/loop$min
        if [ ! -e "$name" ]; then
            echo "Creating loop device $name"
            mknod --mode 0666 $name b 7 $min
        fi
    done
}

create_loop_devices 16

# Workaround for CVE-2022-24765 fix to avoid this warning:
#   fatal: unsafe repository ('/src' is owned by someone else)
git config --global --add safe.directory "$(pwd)"

# Enable IPv6
echo 0 > /proc/sys/net/ipv6/conf/all/disable_ipv6

if [ -z $1 ]; then
    echo "Missing argument 'ENV' (tox environment)."
    echo "Usage: $0 ENV"
    exit 1
fi

env="$1"

make storage
tox -e flake8,test-$env,bench-$env
