# copr build script
#
# The makefile is called as:
# make -f <cloned_repodir>/.copr/Makefile srpm outdir="<outdir>" spec="<spec_path>"
#
# See https://docs.pagure.org/copr.copr/user_documentation.html#make-srpm

srpm:
	# Git is required to create release suffix.
	# python3-setuptools required for building srpm.
	dnf -y install git python3-setuptools

	# Workaround for CVE-2022-24765 fix:
	#
	#	fatal: unsafe repository ('/path' is owned by someone else)
	#
	# Without this build-aux/release is confused, and all builds have same
	# build from tag version (e.g. 2.4.4-1.fc35) instead a master build version
	# (2.4.4-0.202204031154.git300480e.fc35).
	git config --global --add safe.directory "$(shell pwd)"

	$(MAKE) srpm OUTDIR=$(outdir)
