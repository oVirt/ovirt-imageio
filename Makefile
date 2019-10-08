subdirs = common daemon proxy
targets = all check dist srpm rpm

release_suffix := $(shell ./build-aux/release-suffix)

.PHONY: $(targets) $(subdirs) storage clean-storage

$(targets): $(subdirs)

$(subdirs):
	$(MAKE) -C $@ $(MAKECMDGOALS) RELEASE_SUFFIX=$(release_suffix)

clean: $(subdirs)
	rm -rf exported-artifacts/

storage:
	userstorage create storage.py

clean-storage:
	userstorage delete storage.py
