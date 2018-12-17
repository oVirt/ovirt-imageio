#!/bin/bash -xe

# tox too old on EL, so we must install our own
pip install tox

# Make it possibe to run qemu-kvm under mock.
if [ ! -c "/dev/kvm" ]; then
    echo "Creating /dev/kvm"
    mknod /dev/kvm c 10 232
fi

make
make check
