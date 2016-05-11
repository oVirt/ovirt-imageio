requires = python-webob

.PHONY: build check dist srpm rpm

build:

check:
	py.test

dist:
	python setup.py sdist

rpm:
	python setup.py bdist_rpm --requires "$(requires)"

srpm:
	python setup.py bdist_rpm --requires "$(requires)" --source
