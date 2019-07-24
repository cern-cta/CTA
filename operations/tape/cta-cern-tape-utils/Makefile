SPECFILE = cta-cern-tape-utils.spec

FILES = tape-config-generate TapeAdmin.py tape-mount

rpmtopdir := $(shell rpm --eval %_topdir)
rpmbuild  := $(shell [ -x /usr/bin/rpmbuild ] && echo rpmbuild || echo rpm)

PKGNAME = $(shell grep -s '^Name:'    $(SPECFILE) | sed -e 's/Name: *//')
PKGVERS = $(shell grep -s '^Version:' $(SPECFILE) | sed -e 's/Version: *//')

rpm: $(SPECFILE)
	tar cvfz $(rpmtopdir)/SOURCES/$(PKGNAME)-$(PKGVERS).tgz $(FILES)
	cp -f $(SPECFILE) $(rpmtopdir)/SPECS/$(SPECFILE)
	$(rpmbuild) -ba $(SPECFILE)
