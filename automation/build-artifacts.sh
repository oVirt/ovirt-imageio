#!/bin/bash -xe

mkdir -p exported-artifacts
make
make rpm

for dir in common daemon proxy; do \
    cp $dir/dist/* exported-artifacts/
done
