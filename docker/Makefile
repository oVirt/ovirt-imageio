targets := centos-8 fedora-30 fedora-31

.PHONY: $(targets) push

all: $(targets)

$(targets):
	podman build -t imageio-test-$@ -f Dockerfile.$@ .

push:
	for name in $(targets); do \
		podman push imageio-test-$$name ovirtorg/imageio-test-$$name; \
	done
