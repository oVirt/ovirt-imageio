#!/bin/bash -xe

mkdir -p exported-artifacts
make rpm
cp daemon/dist/* exported-artifacts/
