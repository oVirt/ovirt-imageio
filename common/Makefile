RELEASE_SUFFIX := $(shell ../build-aux/release-suffix)
PYTHON_VERSION=2

PYTHON=python$(PYTHON_VERSION)
PACKAGE_NAME=ovirt-imageio-common
PACKAGE_VERSION=$(shell $(PYTHON) ovirt_imageio_common/version.py)

RPM_TOPDIR?=$(PWD)/build/rpm
TAR_NAME=$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz
SPEC_NAME=$(PACKAGE_NAME).spec

GENERATED = $(SPEC_NAME)
METADATA = ovirt_imageio_common/version.py Makefile

.PHONY: build check check-py2 check-py3 dist srpm rpm clean $(SPEC_NAME)

build: $(GENERATED)
	$(PYTHON) setup.py build_ext --inplace

check: check-py$(PYTHON_VERSION)

check-py2:
	tox -e flake8,py27

check-py3:
	tox -e flake8,py36,py37

dist: $(SPEC_NAME)
	rm -f dist/*
	$(PYTHON) setup.py sdist

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
	$(PYTHON) setup.py clean --all
	rm -f MANIFEST
	rm -f $(GENERATED)
	rm -f ovirt_imageio_common/*.so
	rm -rf build
	rm -rf dist

$(GENERATED) : % : %.in $(METADATA)
	@sed \
		-e 's|@PACKAGE_NAME@|$(PACKAGE_NAME)|g' \
		-e 's|@PACKAGE_VERSION@|$(PACKAGE_VERSION)|g' \
		-e 's|@PYTHON_VERSION@|$(PYTHON_VERSION)|g' \
		$< > $@
	@echo "generated $@"
