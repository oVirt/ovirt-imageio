PACKAGE_NAME=ovirt-imageio-daemon
PACKAGE_VERSION=$(shell python2 ovirt_imageio_daemon/version.py)

RPM_TOPDIR?=$(PWD)/build/rpm
TAR_NAME=$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz
SPEC_NAME=$(PACKAGE_NAME).spec

LOGDIR=/var/log/$(PACKAGE_NAME)

GENERATED = \
    $(SPEC_NAME) \
    data/logger.conf

METADATA = ovirt_imageio_daemon/version.py Makefile

.PHONY: build check dist srpm rpm clean

build: $(GENERATED)

check:
	tox

dist: $(SPEC_NAME)
	rm -f dist/*
	python2 setup.py sdist

srpm: dist
	rpmbuild --define="_topdir $(RPM_TOPDIR)" --define="_srcrpmdir dist" \
		--define "release_suffix $(RELEASE_SUFFIX)" \
		-ts dist/$(TAR_NAME)

rpm: srpm
	rpmbuild --define="_topdir $(RPM_TOPDIR)" \
		--define "release_suffix $(RELEASE_SUFFIX)" \
		--rebuild dist/*.src.rpm
	mv $(RPM_TOPDIR)/RPMS/*/* dist/

clean:
	python2 setup.py clean --all
	rm -f MANIFEST
	rm -f $(GENERATED)
	rm -rf build
	rm -rf dist

$(GENERATED) : % : %.in $(METADATA)
	@sed \
		-e 's|@PACKAGE_NAME@|$(PACKAGE_NAME)|g' \
		-e 's|@PACKAGE_VERSION@|$(PACKAGE_VERSION)|g' \
		-e 's|@LOGDIR@|$(LOGDIR)|g' \
		$< > $@
	@echo "generated $@"
