#!/bin/bash -e

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

make storage
make check
