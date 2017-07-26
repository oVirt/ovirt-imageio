TOOL = ovirt-ova

build:
	GOPATH=$(PWD) go build -o $(TOOL) src/main.go

check:
	./$(TOOL) | grep 'It works!'

dist:

srpm:

rpm:

clean:
	rm -f $(TOOL)
