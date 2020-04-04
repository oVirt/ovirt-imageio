#!/bin/bash -xe

# Use to handle flaky tests on Jenkins, when we may have multiple tests
# running on same slave isolated by mock.
export OVIRT_CI=1

# On Fedora 30 and CentOS 8 pip install scripts to /usr/local/bin which
# may not be in PATH.
export PATH="/usr/local/bin:$PATH"

PYTHON=python3

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

print_builder_info() {
    echo -n "release: "
    cat /etc/redhat-release
}

trap teardown EXIT

print_builder_info

# First upgrade pip, since older pip versions have issues with installing
# correct version of requirements.
$PYTHON -m pip install --upgrade pip

# Install development requirements.
$PYTHON -m pip install --upgrade -r docker/requirements.txt

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
