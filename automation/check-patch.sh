#!/bin/bash -xe

# tox too old on EL, so we must install our own
pip install tox

make
make check
