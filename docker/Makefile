targets := centos-8 centos-9 fedora-34 fedora-35

.PHONY: $(targets) push

all: $(targets)

$(targets):
	podman build -t imageio-test-$@ -f Dockerfile.$@ .

push:
	for name in $(targets); do \
		podman push imageio-test-$$name quay.io/ovirt/imageio-test-$$name; \
	done
