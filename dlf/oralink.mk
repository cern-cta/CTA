#
#  Copyright (C) 2003 by CERN/IT/ADC/CA
#  All rights reserved
# 
#  @(#)$RCSfile: oralink.mk,v $ $Revision: 1.1 $ $Date: 2003/08/20 13:08:47 $ CERN IT-ADC Vitaly Motyakov
#
#  Link dlf with Oracle libraries.

include $(ORACLE_HOME)/precomp/lib/env_precomp.mk
PROLDLIBS=$(LLIBCLNTSH) $(LLIBCLIENT) $(LLIBSQL) $(STATICTTLIBS)

dlfserver: $(DLFSERVER_OBJS)
	$(CC) -o dlfserver $(CLDFLAGS) $(DLFSERVER_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)
