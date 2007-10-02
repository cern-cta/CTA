# This Makefile is taken and adapted from the Globus toolkit
#
# * @(#)$RCSfile: Makefile,v $ $Revision: 1.5 $ $Release$ $Date: 2007/10/02 17:15:06 $ $Author: kotlyar $
#

# include makefile_header
USE_MAKEFILE_HEADER=no
# if you whant to use makefile_header than you have to use command
# 1. setenv GLOBUS_LOCATION=/opt/globus;setenv GPT_LOCATION=/opt/gpt
# 2. $GLOBUS_LOCATION/bin/globus-makefile-header -flavor=${FLAVOR} globus_gridftp_server > makefile_header

# include additional makefile headers here
FLAVOR=gcc32dbg
MOPTION=-m32
ifeq ($(shell uname -m), x86_64)
FLAVOR=gcc64dbg
MOPTION=-m64
endif

ifeq ($(USE_MAKEFILE_HEADER), no)
GLOBUS_FLAVOR   =  $(FLAVOR)
GLOBUS_INCLUDES = -I$(GLOBUS_LOCATION)/include/$(GLOBUS_FLAVOR)
GLOBUS_LDFLAGS  = -L$(GLOBUS_LOCATION)/lib $(MOPTION)
GLOBUS_PKG_LIBS = -lglobus_ftp_control_$(GLOBUS_FLAVOR)
GLOBUS_CC 	= /usr/bin/gcc
GLOBUS_CFLAGS   = -g -Wall $(MOPTION)
endif

# add needed cflags here
DSI_CFLAGS=$(GLOBUS_CFLAGS)

# add needed includes here
DSI_INCLUDES=$(GLOBUS_INCLUDES) 

# added needed ldflags here
DSI_LDFLAGS=$(GLOBUS_LDFLAGS) 

# add needed libraries here
DSI_LIBS=$(GLOBUS_PKG_LIBS)

globus_gridftp_server_CASTOR2int.o:
	$(GLOBUS_CC) $(DSI_CFLAGS) $(DSI_INCLUDES) \
		-shared -fPIC -o libglobus_gridftp_server_CASTOR2int_$(FLAVOR).so \
		globus_gridftp_server_CASTOR2int.c \
		$(DSI_LDFLAGS) $(DSI_LIBS)

install:
	cp -f libglobus_gridftp_server_CASTOR2int_$(FLAVOR).so $(DESTDIR)$(GLOBUS_LOCATION)/lib

clean:
	rm -f *.so
