#!/bin/bash -xe

create_loop_devices() {
    # See https://github.com/torvalds/linux/blob/master/Documentation/admin-guide/devices.txt
    local last=$(($1-1))
    local minor
    for minor in `seq 0 $last`; do
        local name=/dev/loop$minor
        if [ ! -e "$name" ]; then
            mknod --mode 0666 $name b 7 $minor
        fi
    done
}

teardown() {
    make clean-storage || echo "WARNING: cleaning storage failed"
}

trap teardown EXIT

# First upgrade pip, since older pip versions have issues with installing
# correct version of requirements.
pip install --upgrade pip

# Install development requirements.
pip install --upgrade -r requirements.txt

# Make it possibe to run qemu-kvm under mock.
if [ ! -c "/dev/kvm" ]; then
    echo "Creating /dev/kvm"
    mknod /dev/kvm c 10 232
fi

# Ensure we have enough loop devices under mock.
create_loop_devices 16

make

make storage

make check
