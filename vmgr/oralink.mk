#
#  Copyright (C) 1999-2000 by CERN/IT/PDP/DM
#  All rights reserved
#
#       @(#)$RCSfile: oralink.mk,v $ $Revision: 1.2 $ $Date: 2000/04/19 12:41:06 $ CERN IT-PDP/DM Jean-Philippe Baud
 
#    Link vmgr with Oracle libraries.

include $(ORACLE_HOME)/precomp/lib/env_precomp.mk
PROLDLIBS=$(LLIBCLNTSH) $(LLIBCLIENT) $(LLIBSQL) $(STATICTTLIBS)

vmgrdaemon: $(VMGRDAEMON_OBJS)
	$(CC) -o vmgrdaemon $(CLDFLAGS) $(VMGRDAEMON_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)
