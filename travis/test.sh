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

make
make storage
make check
