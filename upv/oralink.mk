#
#  Copyright (C) 1999-2000 by CERN/IT/PDP/DM
#  All rights reserved
#
#       @(#)$RCSfile: oralink.mk,v $ $Revision: 1.1 $ $Date: 2002/05/28 09:37:58 $ CERN IT-PDP/DM Ben Couturier 
 
#    Link Cupv with Oracle libraries.

include $(ORACLE_HOME)/precomp/lib/env_precomp.mk
PROLDLIBS=$(LLIBCLNTSH) $(LLIBCLIENT) $(LLIBSQL) $(STATICTTLIBS)

Cupvdaemon: $(CUPVDAEMON_OBJS)
	$(CC) -o Cupvdaemon $(CLDFLAGS) $(CUPVDAEMON_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)
