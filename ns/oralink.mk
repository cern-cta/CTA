#
#  Copyright (C) 1999-2004 by CERN/IT/PDP/DM
#  All rights reserved
#
#       @(#)$RCSfile: oralink.mk,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:23 $ CERN IT-PDP/DM Jean-Philippe Baud
 
#    Link nsdaemon with Oracle libraries.

include $(ORACLE_HOME)/precomp/lib/env_precomp.mk
PROLDLIBS=$(LLIBCLNTSH) $(LLIBCLIENT) $(LLIBSQL) $(STATICTTLIBS)
PROLDLIBS=$(LLIBCLNTSH) $(LLIBCLIENT) $(LLIBSQL)

dpnsdaemon: $(NSDAEMON_OBJS)
	$(CC) -o dpnsdaemon $(CLDFLAGS) $(NSDAEMON_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)
lfcdaemon: $(NSDAEMON_OBJS)
	$(CC) -o lfcdaemon $(CLDFLAGS) $(NSDAEMON_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)
nsdaemon: $(NSDAEMON_OBJS)
	$(CC) -o nsdaemon $(CLDFLAGS) $(NSDAEMON_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)
