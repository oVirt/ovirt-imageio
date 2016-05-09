subdirs = common daemon proxy
targets = all check dist srpm rpm

.PHONY: $(targets) $(subdirs)

$(targets): $(subdirs)

$(subdirs):
	$(MAKE) -C $@ $(MAKECMDGOALS)
