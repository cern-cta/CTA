#######################################################################
## Makefile.am for xCastor2 FS plugin
##
##
## Version info: $Id: Makefile.am,v 1.5 2008/09/15 10:04:02 apeters Exp $
## Checked in by $Author: apeters $
#######################################################################

lib_LTLIBRARIES = libXrdxCastor2Fs.la libXrdxCastor2ServerAcc.la libXrdxCastor2Ofs.la libXrdxCastor2Xmi.la

INCLUDES = -I$(XROOTD_LOCATION)/include/xrootd -I$(CASTOR_SOURCE_LOCATION)/h -I$(CASTOR_SOURCE_LOCATION) -I$(CASTOR_LOCATION) -D_THREAD_SAFE -D_REENTRANT -O -pg -g

bin_PROGRAMS = xrdstager_qry xrdstager_get

###############################################
xrdstager_qry_SOURCES = XrdStagerQry.cc
xrdstager_qry_LDFLAGS = -static -export-dynamic
xrdstager_qry_LDADD   = -L$(XROOTD_LOCATION)/lib$(MARK64)/ -lXrdClient

xrdstager_get_SOURCES = XrdStagerGet.cc
xrdstager_get_LDFLAGS = -static -export-dynamic
xrdstager_get_LDADD   = -L$(XROOTD_LOCATION)/lib$(MARK64)/ -lXrdClient
###############################################


libXrdxCastor2Fs_la_SOURCES = \
	XrdxCastor2Fs.cc \
	XrdxCastor2Config.cc \
	XrdxCastor2Namespace.cc \
	XrdxCastor2ServerAcc.cc \
        XrdxCastor2ServerAcc.hh \
	XrdxCastor2Fs.hh  \
	XrdxCastor2Trace.hh \
	XrdxCastor2Proc.cc \
	XrdxCastor2Proc.hh \
	XrdxCastor2FsConstants.hh \
	XrdxCastor2Timing.hh \
	XrdxCastor2ClientAdmin.hh \
	XrdxCastor2Stager.cc \
	XrdxCastor2Stager.hh \
	XrdxCastor2FsSecurity.hh 



libXrdxCastor2Fs_la_LIBADD = -L$(XROOTD_LOCATION)/lib$(MARK64)/  -lXrdSys -lXrdOuc -lXrdClient -lXrdCms -lXrdOfs -lcrypto -luuid -L$(CASTOR_LOCATION)/lib$(MARK64)/ -lshift
EXTRA_DIST = configure.ac bootstrap.sh xrootd-xcastor2fs.spec xrootd-publickey.spec keys/pkey.pem keys/key.pem etc/xrd.cf x2castorjob x2cp


libXrdxCastor2ServerAcc_la_SOURCES = \
	XrdxCastor2ServerAcc.cc \
	XrdxCastor2ServerAcc.hh 

libXrdxCastor2ServerAcc_la_LIBADD = -L$(XROOTD_LOCATION)/lib$(MARK64)/  -lXrdSys -lXrdOuc -lXrdAcc -lcrypto

libXrdxCastor2Ofs_la_SOURCES = \
	XrdxCastor2Ofs.cc \
	XrdxCastor2Ofs.hh \
	XrdxCastor2OfsProc.cc \
	XrdxCastor2OfsRateLimit.cc  \
	XrdxCastor2OfsRateLimit.hh 

libXrdxCastor2Ofs_la_LIBADD = -L$(XROOTD_LOCATION)/lib$(MARK64)/  -lXrdSys -lXrdOuc -lXrdOfsDev -lXrdClient

libXrdxCastor2Xmi_la_SOURCES = \
	XrdxCastor2Xmi.cc \
	XrdxCastor2Xmi.hh \
	XrdxCastor2XmiTrace.hh

libXrdxCastor2Xmi_la_LIBADD = -L$(XROOTD_LOCATION)/lib$(MARK64)/  -lXrdCms -lXrdOuc -L$(CASTOR_LOCATION)/lib$(MARK64)/ -lshift

client-install:
	mkdir -p $(DESTDIR)/$(prefix)/bin/
	$(INSTALL) -m 755 $(srcdir)/x2cp $(DESTDIR)/$(prefix)/bin/x2cp


install-exec-local: client-install
	mkdir -p $(DESTDIR)/$(prefix)/keys/
	mkdir -p $(DESTDIR)/$(prefix)/bin/
	mkdir -p $(DESTDIR)/etc/
	$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf $(DESTDIR)/etc/xrd.cf
	$(INSTALL) -m 755 $(srcdir)/x2castorjob $(DESTDIR)/$(prefix)/bin/x2castorjob
#	$(INSTALL) -m 644 $(srcdir)/keys/pkey.pem $(DESTDIR)/$(prefix)/keys/ 
#	$(INSTALL) -m 400 $(srcdir)/keys/key.pem $(DESTDIR)/$(prefix)/keys/ 

client-uninstall:
	rm -f $(DESTDIR)/$(prefix)/bin/x2cp

uninstall: client-uninstall
	rm -f $(DESTDIR)/etc/xrd.cf
	rm -f $(DESTDIR)/$(prefix)/bin/x2castorjob

public-key-install:
	mkdir -p $(DESTDIR)/$(prefix)/keys/
	$(INSTALL) -m 644 $(srcdir)/keys/pkey.pem $(DESTDIR)/$(prefix)/keys/

keypair:	
	rm -rf key.pem cert.pem certreq.pem pkey.pem && \
	openssl genrsa -rand 12938467 -out key.pem 512 && \
        openssl req -new -inform PEM -key key.pem -outform PEM -out certreq.pem && \
        openssl x509 -days 3650 -signkey key.pem -in certreq.pem -req -out cert.pem &&\
        openssl x509 -pubkey -in cert.pem > pkey.pem && \
	rm -rf cert.pem certreq.pem && \
	echo "Your new keypair is private-key: ${PWD}/key.pem public-key: ${PWD}/pkey.pem"

rpm:	dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb xrootd-xcastor2fs.spec

pkeyrpm:dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb xrootd-publickey.spec

rpmnodist:	
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb xrootd-xcastor2fs.spec

