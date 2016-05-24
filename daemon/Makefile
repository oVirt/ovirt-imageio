PACKAGE_NAME=ovirt-imageio-daemon
PACKAGE_VERSION=$(shell python ovirt_imageio_daemon/version.py)

RPM_TOPDIR?=$(PWD)/build/rpm
TAR_NAME=$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz
SPEC_NAME=$(PACKAGE_NAME).spec

GENERATED = $(SPEC_NAME)
METADATA = ovirt_imageio_daemon/version.py Makefile

.PHONY: build check dist srpm rpm clean

build: $(GENERATED)

check:
	py.test

dist: $(SPEC_NAME)
	rm -f dist/*
	python setup.py sdist

srpm: dist
	rpmbuild --define="_topdir $(RPM_TOPDIR)" --define="_srcrpmdir dist" \
		--define "release_suffix $(shell build-aux/release-suffix)" \
		-ts dist/$(TAR_NAME)

rpm: srpm
	rpmbuild --define="_topdir $(RPM_TOPDIR)" \
		--define "release_suffix $(shell build-aux/release-suffix)" \
		--rebuild dist/*.src.rpm
	mv $(RPM_TOPDIR)/RPMS/noarch/* dist/

clean:
	python setup.py clean --all
	rm -f MANIFEST
	rm -f $(GENERATED)
	rm -rf build

$(GENERATED) : % : %.in $(METADATA)
	@sed \
		-e 's|@PACKAGE_NAME@|$(PACKAGE_NAME)|g' \
		-e 's|@PACKAGE_VERSION@|$(PACKAGE_VERSION)|g' \
		$< > $@
	@echo "generated $@"
