#######################################################################
## Makefile.am for Castor2 plugin CS2
##
##
## Author: Andreas Joachim Peters <andreas.joachim.peters@cern.ch>
## Initial version: 13.4.2007
##
## Version info: $Id: Makefile.am,v 1.1 2008/02/25 15:15:46 apeters Exp $
## Checked in by $Author: apeters $
#######################################################################

XRDCS2 = `if test -L XrdCS2; then unlink XrdCS2; fi; if ! test -L XrdCS2; then ln -s $(top_srcdir) XrdCS2; fi `

lib_LTLIBRARIES = libXrdCS2.la

INCLUDES = -I$(CASTOR_INCDIR) -I$(CASTOR_INCDIR)/h -I$(XROOTD_LOCATION)/include/xrootd/ -I../ $(XRDCS2) -DCASTOR_214


libXrdCS2_la_SOURCES =  XrdCS2Req.cc \
		        XrdCS2Xmi.cc

libXrdCS2_la_LIBADD = -L$(CASTOR_LIBDIR) -lshift -L$(XROOTD_LOCATION)/lib/ -lXrdNet -lXrdOuc -lXrdSys -lpthread -lnsl -lrt -ldl

libXrdCS2_la_LDFLAGS  = -version-info 2:1:0 -rpath /usr/local/xrootd/lib/

bin_PROGRAMS = XrdCS2d

XrdCS2d_SOURCES = XrdCS2DCM.cc \
        XrdCS2DCM2cs.cc \
        XrdCS2DCMConfig.cc \
        XrdCS2DCMFile.cc\
        XrdCS2DCMMain.cc \
	XrdCS2DCMFile.hh \
	XrdCS2DCM.hh \
	XrdCS2Req.hh \
	XrdCS2Xmi.hh

XrdCS2d_LDADD = -L$(CASTOR_LIBDIR) -lshift -L$(XROOTD_LOCATION)/lib/ -lXrd  -lXrdNet -lXrdOuc -lXrdSys -lpthread -lnsl -lrt -ldl

XrdCS2d_LDFLAGS = -static -export-dynamic

EXTRA_DIST = 	XrdCS2e \
		xrootd-castor2.spec


install-exec-local :
		mkdir -p $(DESTDIR)/$(prefix)/bin/
		touch $(DESTDIR)/$(prefix)/bin/mssgw
		chmod ugo+x $(DESTDIR)/$(prefix)/bin/mssgw
		$(INSTALL) -m 755 $(srcdir)/XrdCS2e $(DESTDIR)/$(prefix)/bin/

