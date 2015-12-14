# default target
.PHONY: all
all: build


PACKAGE_NAME=ovirt-image-proxy
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


.SUFFIXES:
.SUFFIXES: .in

GENERATED = $(shell find . -name '*.in' -printf '%P\n' | sed 's/...$$//')

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


.PHONY: generated-files
generated-files: $(GENERATED)


.PHONY: build
build: generated-files
	/usr/bin/env python setup.py build


.PHONY: install
install: build
ifdef DESTDIR
	/usr/bin/env python setup.py install --skip-build --root $(DESTDIR)
	## TODO install conf/log/service here, in rpm, or have setup.py do it?
else
	# ERROR: DESTDIR is not defined! Please refer to installation notes.
endif



.PHONY: check
check: build
	## TODO or does it depend on install?
	/usr/bin/env python setup.py test


.PHONY: clean
clean:
	/usr/bin/env python setup.py clean
	rm -f version.py?
	rm -f $(PACKAGE_NAME)-*.tar.gz
	rm -f $(PACKAGE_NAME)-*.rpm
	rm -f $(GENERATED)
	rm -rf $(RPM_TOPDIR)


.PHONY: tarball
tarball: dist


.PHONY: dist
dist: $(SPEC_NAME)
	git ls-files | tar --transform='s|^|$(PACKAGE_NAME)-$(PACKAGE_VERSION)/|' \
		--files-from /proc/self/fd/0 -czf $(TAR_NAME) $(SPEC_NAME)


.PHONY: srpm
srpm: tarball
	rpmbuild --define="_topdir $(RPM_TOPDIR)" -ts $(TAR_NAME)


.PHONY: rpm
rpm: srpm
	rpmbuild --define="_topdir $(RPM_TOPDIR)" --rebuild $(SRPM_NAME)
	find $(RPM_TOPDIR)/RPMS -name '*.rpm' | xargs mv -t .
	@echo -e "\nRPMs available:"
	@find . -maxdepth 1 -name '*.rpm' -printf '  %P\n'
	@echo

