PYTHON_VERSION = 2

targets = all check dist srpm rpm

ifeq ($(PYTHON_VERSION), 2)
	subdirs = proxy
else
	subdirs = daemon
endif

release_suffix := $(shell ./build-aux/release-suffix)

.PHONY: $(targets) $(subdirs) storage clean-storage

$(targets): $(subdirs)

$(subdirs):
	$(MAKE) -C $@ $(MAKECMDGOALS) \
		RELEASE_SUFFIX=$(release_suffix) \
		PYTHON_VERSION=$(PYTHON_VERSION)

clean: $(subdirs)
	rm -rf exported-artifacts/

storage:
	userstorage create storage.py

clean-storage:
	userstorage delete storage.py
