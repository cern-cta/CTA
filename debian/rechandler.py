##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                        Example For Recall Policy File                  #
#                                                                        #
##########################################################################

# Modules

# The policy-function which decides whether or not a specific recall-mount
# should take place right-now.
#
# @param vid                The VID of the tape to be mounted for recall.
# @param numSegments        The total number of segements which will be
#                           recalled during this mount.
# @param totalBytes         The sum of the sizes of all of the files which will
#                           be recalled during the mount.
# @param ageOfOldestSegment The age in seconds of the oldest segment waiting to
#                           be recalled during the mount.
# @param priority           The priority of the tape to be mounted.
# @param nbMountsForRecall  The total number of recall-mounts which are either
#                           passed by the rechandler-policy, waiting for a
#                           drive, waiting to be mounted or are already mounted.
# @return boolean           Indicating recall or not the tape.

def globalRecallPolicy(vid, numSegments, totalBytes, ageOfOldestSegment, priority, nbMountsForRecall):
      return 1

# End-of-File
