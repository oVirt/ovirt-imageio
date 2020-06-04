targets = all check dist srpm rpm

subdirs = daemon

release := $(shell ./build-aux/release)

.PHONY: $(targets) $(subdirs) storage clean-storage

$(targets): $(subdirs)

$(subdirs):
	$(MAKE) -C $@ $(MAKECMDGOALS) RELEASE=$(release)

clean: $(subdirs)
	rm -rf exported-artifacts/

storage:
	userstorage create storage.py

clean-storage:
	userstorage delete storage.py
