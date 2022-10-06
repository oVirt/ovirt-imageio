# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

# copr build script
#
# The makefile is called as:
# make -f <cloned_repodir>/.copr/Makefile srpm outdir="<outdir>" spec="<spec_path>"
#
# See https://docs.pagure.org/copr.copr/user_documentation.html#make-srpm

srpm:
	# Git is required to create release suffix.
	# python3-setuptools required for building srpm.
	dnf -y install git python3-setuptools

	$(MAKE) srpm OUTDIR=$(outdir)
