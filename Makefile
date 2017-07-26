subdirs = common daemon proxy ova
targets = all check dist srpm rpm

.PHONY: $(targets) $(subdirs)

$(targets): $(subdirs)

$(subdirs):
	$(MAKE) -C $@ $(MAKECMDGOALS)

clean: $(subdirs)
	rm -rf exported-artifacts/
