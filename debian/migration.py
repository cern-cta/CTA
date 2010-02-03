##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                        Example For Migration Policy File               #
#                                                                        #
##########################################################################

# Modules
import sys
import os

def defaultMigrationPolicy(tapepool, castorfilename,copynb,fileId,fileSize,fileMode,uid,gid,aTime,mTime,cTime,fileClass):
        return 1

# End-of-File
