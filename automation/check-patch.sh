#!/bin/bash -xe

# tox too old on EL, so we must install our own
pip install tox

make

# We cannot use "make check" as we need to pass arguments to the underlying
# py.test call.
# TODO: add this option to the main makefile.

for subdir in common daemon proxy; do
    pushd $subdir
    tox -- -m \'not noci\'
    popd
done
