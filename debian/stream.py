##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                        Example For Stream Policy File                  #
#                                                                        #
##########################################################################

import time

# Stream-policy Python-function which always returns 1
#
# @param streamId            Input unsigned 64-bit integer: The database ID of
#                            the stream to be considered.
# @param numTapeCopies       Input unsigned 64-bit integer: The number of
#                            tape-copies attached to the stream.
# @param totalBytes          Input unsigned 64-bit integer: The sum of the
#                            sizes of all the attached tape-copies.
# @param ageOfOldestTapeCopy Input unsigned 64-bit integer: The age of the
#                            oldest tape-copy attached to the stream as the
#                            number of seconds since the UNIX epoch
#                            (00:00:00 UTC on 1 January 1970).
# @param tapePoolId          Input unsigned 64-bit integer: The database ID of
#                            the tape-pool to which the stream belongs.
# @param tapePoolName        Input string: The name of the tape-pool to which
#                            the stream belongs.
# @param tapePoolNbRunningStreams Input unsigned 64-bit integer: The number of
#                            running streams attached to the tape-pool to which
#                            the stream to be considered is also attached.
# @param svcClassId          Input unsigned 64-bit integer: The database ID of
#                            the service-class to which the stream belongs.
# @param svcClassName        Input string: The name of the service-class to
#                            which the stream belongs.
# @param svcClassNbDrives    Input unsigned 64-bit integer: The nbDrives
#                            attribute of the service-class to which the stream
#                            belongs. This is therefore the maximum number of
#                            running-streams the service-class SHOULD HAVE at any
#                            single moment in time. Please note the number of
#                            created streams may be GREATER than the number that
#                            should be running.
# @return                    Output integer: Non-zero if the stream being
#                            considered should start running and zero if it
#                            should not.
def streamPolicyAlwaysReturning1(streamId,numTapeCopies,totalBytes,ageOfOldestTapeCopy,tapePoolId,tapePoolName,tapePoolNbRunningStreams,svcClassId,svcClassName,svcClassNbDrives):

  returnCode = 1

  # Log input and decision
  logFile = open('/tmp/streamPolicy.log','a')
  logFile.writelines("StreamPolicy called "+time.strftime("%d %b %Y %H:%M:%S", time.localtime())+" streamId="+str(streamId)+",numTapeCopies="+str(numTapeCopies)+",totalBytes="+str(totalBytes)+",ageOfOldestTapeCopy="+str(ageOfOldestTapeCopy)+",tapePoolId="+str(tapePoolId)+",tapePoolName="+tapePoolName+",tapePoolNbRunningStreams="+str(tapePoolNbRunningStreams)+",svcClassId="+str(svcClassId)+",svcClassName="+svcClassName+",svcClassNbDrives="+str(svcClassNbDrives)+",returnCode="+str(returnCode)+'\n')
  logFile.close()

  return returnCode

#streamPolicyAlwaysReturning1(1,2,3,4,5,"six",7,8,"nine",10);

# End-of-File
