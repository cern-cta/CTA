#
#  Copyright (C) 1999 by CERN/IT/PDP/DM
#  All rights reserved
#
#       @(#)$RCSfile: oralink.mk,v $ $Revision: 1.1 $ $Date: 2000/01/23 13:34:51 $ CERN IT-PDP/DM Jean-Philippe Baud
 
#    Link vmgr with Oracle libraries.

include $(ORACLE_HOME)/precomp/lib/env_precomp.mk
PROLDLIBS=$(LLIBCLNTSH) $(LLIBCLIENT) $(LLIBSQL) $(STATICTTLIBS)

vmgr: $(VMGRDAEMON_OBJS)
	$(CC) -o vmgr $(CLDFLAGS) $(VMGRDAEMON_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)
