# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

prefix := ovirt-imageio-test
targets := centos-8 centos-9 fedora-36 fedora-37 fedora-38

.PHONY: $(targets) push

all: $(targets)

$(targets):
	podman build -t $(prefix):$@ -f $@.containerfile .

push:
	for name in $(targets); do \
		podman push $(prefix):$$name quay.io/ovirt/$(prefix):$$name; \
	done
