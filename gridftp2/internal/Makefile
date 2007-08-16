# This Makefile is taken and adapted from the Globus toolkit
#
# * @(#)$RCSfile: Makefile,v $ $Revision: 1.3 $ $Release$ $Date: 2007/08/16 14:55:54 $ $Author: kotlyar $
#

include makefile_header
# include additional makefile headers here

# add needed cflags here
DSI_CFLAGS=$(GLOBUS_CFLAGS)

# add needed includes here
DSI_INCLUDES=$(GLOBUS_INCLUDES) 

# added needed ldflags here
DSI_LDFLAGS=$(GLOBUS_LDFLAGS)

# add needed libraries here
DSI_LIBS=

FLAVOR=gcc32dbg
ifeq ($(shell uname -m), x86_64)
FLAVOR=gcc64dbg
endif

globus_gridftp_server_CASTOR2int.o:
	$(GLOBUS_CC) $(DSI_CFLAGS) $(DSI_INCLUDES) \
		-shared -fPIC -o libglobus_gridftp_server_CASTOR2int_$(FLAVOR).so \
		globus_gridftp_server_CASTOR2int.c \
		$(DSI_LDFLAGS) $(DSI_LIBS)

install:
	cp -f libglobus_gridftp_server_CASTOR2int_$(FLAVOR).so $(GLOBUS_LOCATION)/lib

clean:
	rm -f *.so
