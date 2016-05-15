PACKAGE_NAME=ovirt-imageio-proxy
PROGRAM_NAME=ovirt-image-proxy
PACKAGE_VERSION=$(shell python version.py --version)
RPM_RELEASE=$(shell python version.py --release)$(release_suffix)
VDSM_USER=vdsm
VDSM_GROUP=kvm

PWD=$(shell bash -c "pwd -P")
RPM_DIST=$(shell rpm --eval '%dist')
PROXY_CONFDIR=$(shell rpm --eval '%_sysconfdir')/$(PACKAGE_NAME)
PROXY_LOGDIR=$(shell rpm --eval '%_localstatedir')/log/$(PACKAGE_NAME)
RPM_TOPDIR?=$(PWD)/tmp.repos
SPEC_NAME=$(PACKAGE_NAME).spec
TAR_NAME=$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz
SRPM_NAME=$(RPM_TOPDIR)/SRPMS/$(PACKAGE_NAME)-$(PACKAGE_VERSION)-$(RPM_RELEASE)$(RPM_DIST).src.rpm

GENERATED = $(shell find . -name '*.in' -printf '%P\n' | sed 's/...$$//')

requires = ovirt-imageio-common python-requests m2crypto

.SUFFIXES:
.SUFFIXES: .in

.PHONY: all generated-files build install clean check dist srpm rpm

build: $(GENERATED)
	python setup.py build

check:
	py.test

dist:
	python setup.py sdist

srpm:
	python setup.py bdist_rpm --requires "$(requires)" --source

rpm:
	python setup.py bdist_rpm --requires "$(requires)"

install:
ifdef DESTDIR
	python setup.py install --skip-build --root $(DESTDIR)
	## TODO install conf/log/service here, in rpm, or have setup.py do it?
else
	# ERROR: DESTDIR is not defined! Please refer to installation notes.
endif

clean:
	python setup.py clean
	rm -f version.py?
	rm -f $(PACKAGE_NAME)-*.tar.gz
	rm -f $(PACKAGE_NAME)-*.rpm
	rm -f $(GENERATED)
	rm -rf $(RPM_TOPDIR)

$(GENERATED) : % : %.in Makefile
	@sed \
		-e 's|@PACKAGE_NAME@|$(PACKAGE_NAME)|g' \
		-e 's|@PACKAGE_VERSION@|$(PACKAGE_VERSION)|g' \
		-e 's|@PACKAGE_RPM_RELEASE@|$(RPM_RELEASE)|g' \
		-e 's|@PROGRAM_NAME@|$(PROGRAM_NAME)|g' \
		-e 's|@VDSM_USER@|$(VDSM_USER)|g' \
		-e 's|@VDSM_GROUP@|$(VDSM_GROUP)|g' \
		-e 's|@PROXY_CONFDIR@|$(PROXY_CONFDIR)|g' \
		-e 's|@PROXY_LOGDIR@|$(PROXY_LOGDIR)|g' \
		$< > $@
	@echo "generated $@"
