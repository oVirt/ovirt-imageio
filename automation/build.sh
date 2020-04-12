#!/bin/bash -xe

exported_artifacts="$PWD/exported-artifacts"

mkdir -p "$exported_artifacts"
make rpm OUTDIR="$exported_artifacts"
