#!/bin/sh -e

# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

EXPORT_DIR="${EXPORT_DIR:-exported-artifacts}"

mkdir -p "${EXPORT_DIR}"

# Workaround for CVE-2022-24765 fix:
#   fatal: unsafe repository ('/__w/ovirt-imageio/ovirt-imageio' is owned by
#   someone else)
# Required for correct rpm version when not building from tag.
git config --global --add safe.directory "$(pwd)"

make rpm OUTDIR="${EXPORT_DIR}"
createrepo_c "${EXPORT_DIR}"
