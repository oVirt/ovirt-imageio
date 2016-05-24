PACKAGE_NAME=ovirt-imageio-proxy
PROGRAM_NAME=ovirt-imageio-proxy
PACKAGE_VERSION=$(shell python ovirt_imageio_proxy/version.py --version)

# TODO: remove rpm dependencies
PROXY_CONFDIR=$(shell rpm --eval '%_sysconfdir')/$(PACKAGE_NAME)
PROXY_LOGDIR=$(shell rpm --eval '%_localstatedir')/log/$(PACKAGE_NAME)
RPM_TOPDIR?=$(PWD)/build/rpm
SPEC_NAME=$(PACKAGE_NAME).spec
TAR_NAME=$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz

GENERATED = \
	$(SPEC_NAME) \
	data/logger.conf \
	ovirt_imageio_proxy/constants.py \
	$(NULL)

METADATA = ovirt_imageio_proxy/version.py Makefile

.SUFFIXES:
.SUFFIXES: .in

.PHONY: all generated-files build install clean check dist srpm rpm

build: $(GENERATED)
	python setup.py build

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

install:
ifdef DESTDIR
	python setup.py install --skip-build --root $(DESTDIR)
else
	# ERROR: DESTDIR is not defined! Please refer to installation notes.
endif

clean:
	python setup.py clean --all
	rm -f MANIFEST
	rm -f $(GENERATED)
	rm -rf build

$(GENERATED) : % : %.in $(METADATA)
	@sed \
		-e 's|@PACKAGE_NAME@|$(PACKAGE_NAME)|g' \
		-e 's|@PACKAGE_VERSION@|$(PACKAGE_VERSION)|g' \
		-e 's|@PROGRAM_NAME@|$(PROGRAM_NAME)|g' \
		-e 's|@PROXY_CONFDIR@|$(PROXY_CONFDIR)|g' \
		-e 's|@PROXY_LOGDIR@|$(PROXY_LOGDIR)|g' \
		$< > $@
	@echo "generated $@"
