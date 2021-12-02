#!/bin/sh -e

EXPORT_DIR="${EXPORT_DIR:-exported-artifacts}"

mkdir -p "${EXPORT_DIR}"
make rpm OUTDIR="${EXPORT_DIR}"
createrepo_c "${EXPORT_DIR}"
