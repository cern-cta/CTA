# This Makefile is taken and adapted from the Globus toolkit
#
# * @(#)$RCSfile: Makefile,v $ $Revision: 1.1 $ $Release$ $Date: 2006/10/18 07:46:25 $ $Author: itglp $
#

include makefile_header
# include additional makefile headers here

# add needed cflags here
DSI_CFLAGS=$(GLOBUS_CFLAGS)

# add needed includes here
DSI_INCLUDES=$(GLOBUS_INCLUDES) -I ../../h 

# added needed ldflags here
DSI_LDFLAGS=$(GLOBUS_LDFLAGS)

# add needed libraries here
DSI_LIBS=-lshift

FLAVOR=gcc32dbg

globus_gridftp_server_CASTOR2ext.o:
	$(GLOBUS_CC) $(DSI_CFLAGS) $(DSI_INCLUDES) \
		-shared -o libglobus_gridftp_server_CASTOR2ext_$(FLAVOR).so \
		globus_gridftp_server_CASTOR2.c \
		$(DSI_LDFLAGS) $(DSI_LIBS)

install:
	cp -f libglobus_gridftp_server_CASTOR2ext_$(FLAVOR).so $(GLOBUS_LOCATION)/lib

clean:
	rm -f *.so
