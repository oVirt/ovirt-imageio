#!/bin/bash -xe

mkdir -p exported-artifacts
make
make rpm
cp daemon/dist/* exported-artifacts/
