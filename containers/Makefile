# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

prefix := ovirt-imageio-test
targets := centos-9 centos-10 fedora-40 fedora-41

.PHONY: $(targets) push

all: $(targets)

$(targets):
	podman build -t $(prefix):$@ -f $@.containerfile .

push:
	for name in $(targets); do \
		podman push $(prefix):$$name quay.io/ovirt/$(prefix):$$name; \
	done
