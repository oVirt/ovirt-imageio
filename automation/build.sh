#!/bin/bash -xe

mkdir -p exported-artifacts
make PYTHON_VERSION=$PYTHON_VERSION
make PYTHON_VERSION=$PYTHON_VERSION rpm

for dir in daemon proxy; do \
    if [ -d "$dir/dist" ]; then
        cp $dir/dist/* exported-artifacts/
    fi
done
