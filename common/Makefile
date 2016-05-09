.PHONY: rpm
rpm:
	python setup.py bdist_rpm --requires python-webob


.PHONY: srpm
srpm:
	python setup.py bdist_rpm --requires python-webob --source


.PHONY: dist
dist:
	python setup.py sdist


.PHONY: check
check:
	py.test
