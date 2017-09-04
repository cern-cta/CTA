SPECFILE    = $(shell find -maxdepth 1 -type f -name *.spec)
PACKAGE     = $(shell awk '$$1 == "Name:"     { print $$2 }' $(SPECFILE) )
VERSION     = $(shell awk '$$1 == "Version:"  { print $$2 }' $(SPECFILE) )
RELEASE     = $(shell awk '$$1 == "Release:"  { print $$2 }' $(SPECFILE) )
TARFILE     = $(PACKAGE)-$(VERSION).tar.gz
RPMTOPDIR   = $(shell rpm --eval '%{_topdir}')
BUILDARCH   = $(shell awk '$$1 == "BuildArch:"  { print $$2 }' $(SPECFILE) )
BUILTRPM    = $(RPMTOPDIR)/RPMS/$(BUILDARCH)/$(PACKAGE)-$(VERSION)-$(RELEASE).$(BUILDARCH).rpm

all: $(TARFILE)


$(TARFILE):
	tar cvzf $(TARFILE) --hard-dereference --dereference --exclude-vcs --transform 's,^,$(PACKAGE)-$(VERSION)/,' *

clean:
	rm $(TARFILE)

build: $(TARFILE) $(SPECFILE)
	mv $(TARFILE) $(RPMTOPDIR)/SOURCES
	rpmbuild -ba $(SPECFILE)

deb_build: build
	sudo alien $(BUILTRPM) --scripts -k
