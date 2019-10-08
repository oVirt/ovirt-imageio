#!/bin/bash -e

make PYTHON_VERSION=$PYTHON_VERSION
make storage
make PYTHON_VERSION=$PYTHON_VERSION check
