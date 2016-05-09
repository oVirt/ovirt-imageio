#!/bin/bash -xe

# pytest is too old on Fedora and EL, so we must install our own
pip install pytest pytest-cov requests-mock

make

# We cannot use "make check" as we need to pass arguments to the underlying
# py.test call.
# TODO: add this option to the main makefile.

for subdir in common daemon proxy; do
    pushd $subdir
    py.test -m "not noci"
    popd
done
