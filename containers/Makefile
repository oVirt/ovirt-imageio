# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

targets := centos-8 centos-9 fedora-35 fedora-36

.PHONY: $(targets) push

all: $(targets)

$(targets):
	podman build -t imageio-test-$@ -f $@.containerfile .

push:
	for name in $(targets); do \
		podman push imageio-test-$$name quay.io/ovirt/imageio-test-$$name; \
	done
