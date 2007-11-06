##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                        Example For Recall Policy File                  #
#                                                                        #
##########################################################################

# Modules
import sys
import os

MAX_TIME=os.gettime()
MAX_BYTE=0
MAX_NUMFILE=0


def defaultRecallPolicy(vid, numFiles, dataVolume, oldestTime):
    return 1;

# End-of-File
