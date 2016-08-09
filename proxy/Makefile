PACKAGE_NAME=ovirt-imageio-proxy
PROGRAM_NAME=ovirt-imageio-proxy
PACKAGE_VERSION=$(shell python ovirt_imageio_proxy/version.py --version)

# TODO: remove rpm dependencies
PROXY_CONFDIR=$(shell rpm --eval '%_sysconfdir')/$(PACKAGE_NAME)
PROXY_LOGDIR=$(shell rpm --eval '%_localstatedir')/log/$(PACKAGE_NAME)
RPM_TOPDIR?=$(PWD)/build/rpm
SPEC_NAME=$(PACKAGE_NAME).spec
TAR_NAME=$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz

# The following must be in sync with ovirt-engine spec file definitions
ENGINE_NAME=ovirt-engine
ENGINE_SYSCONF_DIR=$(shell rpm --eval '%_sysconfdir')
ENGINE_PKG_PKI_DIR=$(ENGINE_PREFIX)$(ENGINE_SYSCONF_DIR)/pki/$(ENGINE_NAME)

GENERATED = \
	$(SPEC_NAME) \
	data/logger.conf \
	ovirt_imageio_proxy/constants.py \
	setup/ovirt_engine_setup/ovirt_imageio_proxy/config.py \
	$(NULL)

METADATA = ovirt_imageio_proxy/version.py Makefile

.SUFFIXES:
.SUFFIXES: .in

.PHONY: all generated-files build install install-dev clean check dist srpm rpm

build: $(GENERATED)
	python setup.py build

check:
	tox

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

install-dev:
	python setup.py install
	# TODO: Remove duplication from spec
	install -Dm 0644 data/logger.conf /etc/ovirt-imageio-proxy
	install -Dm 0644 setup/data/ovirt-imageio-proxy.xml.in $(ENGINE_PREFIX)/share/ovirt-engine/firewalld/ovirt-imageio-proxy/ovirt-imageio-proxy.xml.in
	find setup/plugins setup/ovirt_engine_setup -name '*.py' -exec install -Dm 0644 {} $(ENGINE_PREFIX)/share/ovirt-engine/{} \;

clean:
	python setup.py clean --all
	rm -f MANIFEST
	rm -f $(GENERATED)
	rm -rf build
	rm -rf dist

$(GENERATED) : % : %.in $(METADATA)
	@sed \
		-e 's|@PACKAGE_NAME@|$(PACKAGE_NAME)|g' \
		-e 's|@PACKAGE_VERSION@|$(PACKAGE_VERSION)|g' \
		-e 's|@PROGRAM_NAME@|$(PROGRAM_NAME)|g' \
		-e 's|@PROXY_CONFDIR@|$(PROXY_CONFDIR)|g' \
		-e 's|@PROXY_LOGDIR@|$(PROXY_LOGDIR)|g' \
		-e 's|@ENGINE_NAME@|$(ENGINE_NAME)|g' \
		-e 's|@ENGINE_PKI@|$(ENGINE_PKG_PKI_DIR)|g' \
		-e 's|@PROXY_CONF_FILE@|$(PROXY_CONFDIR)/ovirt-imageio-proxy.conf|g' \
		$< > $@
	@echo "generated $@"
