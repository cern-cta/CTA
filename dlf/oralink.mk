#-----------------------------------------------------------------------------------------------------#
#                                                                                                     #
# oracle.mk - Castor Distribution Logging Facility                                                    #
# Copyright (C) 2005 CERN IT/FIO (castor-dev@cern.ch)                                                 #
#                                                                                                     #
# This program free software; you can redistribute it and/or modify it under the terms of the GNU     #
# General Public License as published by the Free Software Foundation; either version 2 of the        #
# License, or (at your option) any later version.                                                     #
#                                                                                                     #
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without   #
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU      #
# General Public License for more details.                                                            #
#                                                                                                     #
# You should have received a copy of the GNU General Public License along with this program; if not,  #
# write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,   #
# USA.                                                                                                #
#                                                                                                     #
#-----------------------------------------------------------------------------------------------------#

#
# Link to Oracle libraries
#

include $(ORACLE_HOME)/precomp/lib/env_precomp.mk
PROLDLIBS=$(LLIBCLNTSH) $(LLIBCLIENT) $(LLIBSQL) $(STATICTTLIBS)

dlfserver: $(DLFSERVER_OBJS)
	$(CC) -o dlfserver $(CLDFLAGS) $(DLFSERVER_OBJS) $(LIBS) -L$(LIBHOME) $(PROLDLIBS)


#-- End-of-File --------------------------------------------------------------------------------------#
