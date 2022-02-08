RELEASE := $(shell ./build-aux/release)
PACKAGE_NAME=ovirt-imageio
PACKAGE_VERSION=$(shell python3 ovirt_imageio/_internal/version.py)
OUTDIR=dist

RPM_TOPDIR?=$(PWD)/build/rpm
TAR_NAME=$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz
SPEC_NAME=$(PACKAGE_NAME).spec

GENERATED = \
    $(SPEC_NAME)

METADATA = ovirt_imageio/_internal/version.py Makefile

.PHONY: build check dist srpm rpm clean storage clean-storage $(SPEC_NAME)

build:
	python3 setup.py build_ext --inplace

check:
	tox

dist: $(GENERATED)
	python3 setup.py sdist --dist-dir "$(OUTDIR)"

srpm: dist
	rpmbuild --define="_topdir $(RPM_TOPDIR)" \
		--define="_srcrpmdir $(OUTDIR)" \
		-ts "$(OUTDIR)/$(TAR_NAME)"

rpm: srpm
	rpmbuild --define="_topdir $(RPM_TOPDIR)" \
		-rb "$(OUTDIR)/$(PACKAGE_NAME)-$(PACKAGE_VERSION)"*.src.rpm
	mv $(RPM_TOPDIR)/RPMS/*/* "$(OUTDIR)"

clean:
	python3 setup.py clean --all
	rm -f MANIFEST
	rm -f $(GENERATED)
	rm -f ovirt_imageio/_internal/*.so
	rm -rf build
	rm -rf dist

storage:
	userstorage create storage.py

clean-storage:
	userstorage delete storage.py

$(GENERATED) : % : %.in $(METADATA)
	@sed \
		-e 's|@PACKAGE_NAME@|$(PACKAGE_NAME)|g' \
		-e 's|@PACKAGE_VERSION@|$(PACKAGE_VERSION)|g' \
		-e 's|@RELEASE@|$(RELEASE)|g' \
		$< > $@
	@echo "generated $@"
