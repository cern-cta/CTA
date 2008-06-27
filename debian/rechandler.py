##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                        Example For Recall Policy File                  #
#                                                                        #
##########################################################################

# Modules
import sys
import os

def globalRecallPolicy(vid, numFiles, dataVolume, oldestTime, priority):
    return priority

# End-of-File
