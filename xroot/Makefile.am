#######################################################################
## Makefile.am for xCastor2 FS plugin
##
##
## Version info: $Id: Makefile.am,v 1.17 2009/07/06 08:27:11 apeters Exp $
## Checked in by $Author: apeters $
#######################################################################

lib_LTLIBRARIES = libXrdxCastor2Fs.la libXrdxCastor2ServerAcc.la libXrdxCastor2Ofs.la libXrdxCastor2Xmi.la

INCLUDES = -I$(XROOTD_LOCATION)/include/xrootd -I$(CASTOR_SOURCE_LOCATION)/h -I$(CASTOR_SOURCE_LOCATION) -I$(CASTOR_LOCATION) -D_THREAD_SAFE -D_REENTRANT -O -pg -g

bin_PROGRAMS = xrdstager_qry xrdstager_get x2castormond

x2castormond_SOURCES = x2castormond.c

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
EXTRA_DIST = configure.ac bootstrap.sh xrootd-xcastor2fs.spec xrootd-xcastor2util.spec xrootd-publickey.spec etc/xrd.cf etc/xrd.cf.meta x2cp x2proc etc/init.d/x2castormond x2castormonitoring.pl etc/logrotate.d/x2castormond etc/sysconfig/x2castormond.example etc/logrotate.d/xrd etc/init.d/xrd etc/init.d/cmsd etc/sysconfig/xrd.example


libXrdxCastor2ServerAcc_la_SOURCES = \
	XrdxCastor2ServerAcc.cc \
	XrdxCastor2ServerAcc.hh 

libXrdxCastor2ServerAcc_la_LIBADD = -L$(XROOTD_LOCATION)/lib$(MARK64)/  -lXrdSys -lXrdOuc -lXrdAcc -lcrypto 

libXrdxCastor2Ofs_la_SOURCES = \
	XrdxCastor2Ofs.cc \
	XrdxCastor2Ofs.hh \
	XrdxCastor2OfsProc.cc \
	XrdxCastor2OfsRateLimit.cc  \
	XrdxCastor2OfsRateLimit.hh \
        XrdxCastor2OfsFSctl.cc \
        XrdxCastor2OfsTransfer.cc 

libXrdxCastor2Ofs_la_LIBADD = -L$(XROOTD_LOCATION)/lib$(MARK64)/  -lXrdSys -lXrdOuc -lXrdOfs -lXrdClient -lXrdTransferManager -lz -L$(CASTOR_LOCATION)/lib$(MARK64)/ -lshift -luuid

libXrdxCastor2Xmi_la_SOURCES = \
	XrdxCastor2Xmi.cc \
	XrdxCastor2Xmi.hh \
	XrdxCastor2XmiTrace.hh \
	XrdxCastor2Fs.hh \
	XrdxCastor2Stager.hh \
	XrdxCastor2Trace.hh 

libXrdxCastor2Xmi_la_LIBADD = -L$(XROOTD_LOCATION)/lib$(MARK64)/  -lXrdCms -lXrdOuc -L$(CASTOR_LOCATION)/lib$(MARK64)/ -lshift libXrdxCastor2Fs.la

client-install:
	mkdir -p $(DESTDIR)/$(prefix)/bin/ 
	$(INSTALL) -m 755 $(srcdir)/x2cp $(DESTDIR)/$(prefix)/bin/x2cp

mon-install:
	mkdir -p $(DESTDIR)/$(prefix)/bin/	
	mkdir -p $(DESTDIR)/etc/init.d/
	mkdir -p $(DESTDIR)/etc/logrotate.d/
	mkdir -p $(DESTDIR)/etc/sysconfig/
	$(INSTALL) -m 755 $(srcdir)/x2castormonitoring.pl $(DESTDIR)/$(prefix)/bin/x2castormonitoring.pl
	$(INSTALL) -m 755 $(srcdir)/x2castormond $(DESTDIR)/$(prefix)/bin/x2castormond
	$(INSTALL) -m 755 $(srcdir)/etc/init.d/x2castormond $(DESTDIR)/etc/init.d/x2castormond
	$(INSTALL) -m 644 $(srcdir)/etc/logrotate.d/x2castormond $(DESTDIR)/etc/logrotate.d/x2castormond
	$(INSTALL) -m 644 $(srcdir)/etc/sysconfig/x2castormond.example $(DESTDIR)/etc/sysconfig/x2castormond.example

install-exec-local: client-install
	mkdir -p $(DESTDIR)/$(prefix)/bin
	mkdir -p $(DESTDIR)/var/log/xroot/server/proc
	mkdir -p $(DESTDIR)/var/log/xroot/empty
	mkdir -p $(DESTDIR)/var/spool/xroot/admin
	mkdir -p $(DESTDIR)/var/spool/xroot/core
	mkdir -p $(DESTDIR)/etc/rc.d/init.d
	mkdir -p $(DESTDIR)/etc/logrotate.d
	mkdir -p $(DESTDIR)/etc/sysconfig
	$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf $(DESTDIR)/etc/xrd.cf
	$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.meta $(DESTDIR)/etc/xrd.cf.meta
	$(INSTALL) -m 755 $(srcdir)/x2proc $(DESTDIR)/$(prefix)/bin/x2proc
	$(INSTALL) -m 755 $(srcdir)/etc/init.d/xrd $(DESTDIR)/etc/rc.d/init.d/xrd
	$(INSTALL) -m 755 $(srcdir)/etc/init.d/cmsd $(DESTDIR)/etc/rc.d/init.d/cmsd
	$(INSTALL) -m 644 $(srcdir)/etc/logrotate.d/xrd $(DESTDIR)/etc/logrotate.d/xrd
	$(INSTALL) -m 644 $(srcdir)/etc/sysconfig/xrd.example $(DESTDIR)/etc/sysconfig/xrd.example

client-uninstall:
	rm -f $(DESTDIR)/$(prefix)/bin/x2cp

mon-uninstall:
	rm -f $(DESTDIR)/etc/init.d/x2castormond
	rm -f $(DESTDIR)/$(prefix)/bin/x2castormonitoring.pl
	rm -f $(DESTDIR)/etc/logrotate.d/x2castormond
	rm -f $(DESTDIR)/$(prefix)/bin/x2castormond
	rm -f $(DESTDIR)/etc/sysconfig/x2castormond.example

uninstall: client-uninstall mon-uninstall
	rm -f $(DESTDIR)/etc/xrd.cf
	rm -f $(DESTDIR)/etc/xrd.cf.meta
	rm -f $(DESTDIR)/etc/rc.d/init.d/xrd
	rm -f $(DESTDIR)/etc/rc.d/init.d/cmsd
	rm -f $(DESTDIR)/etc/logrotate.d/xrd
	rm -f $(DESTDIR)/etc/sysconfig/xrd.example

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

monrpm:dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb xrootd-castormon.spec

rpmnodist:	
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb xrootd-xcastor2fs.spec

utilrpm:dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb xrootd-xcastor2util.spec
