targets := centos-7 fedora-29

.PHONY: $(targets) push

all: $(targets)

$(targets):
	podman build -t imageio-test-$@ -f Dockerfile.$@ .

push:
	for name in $(targets); do \
		podman push imageio-test-$$name ovirtorg/imageio-test-$$name; \
	done
