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
# @param nbRunningStreams    Input unsigned 64-bit integer: The number of
#                            running streams attached to the tape-pool to which
#                            the stream to be considered is also attached.
# @param svcClassName        Input string: The name of the service-class to
#                            which the stream belongs.
# @return                    Output integer: Non-zero if the stream being
#                            considered should be started and zero if it should
#                            not.
def streamPolicyAlwaysReturning1(streamId,numTapeCopies,totalBytes,ageOfOldestTapeCopy,tapePoolId,tapePoolName,nbRunningStreams,svcClassName):
  return 1;

# The default stream-policy Python-function
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
# @param nbRunningStreams    Input unsigned 64-bit integer: The number of
#                            running streams attached to the tape-pool to which
#                            the stream to be considered is also attached.
# @param svcClassName        Input string: The name of the service-class to
#                            which the stream belongs.
# @return                    Output integer: Non-zero if the stream being
#                            considered should be started and zero if it should
#                            not.
def defaultStreamPolicy(streamId,numTapeCopies,totalBytes,ageOfOldestTapeCopy,tapePoolId,tapePoolName,nbRunningStreams,svcClassName):
  """the defaultStreamPolicy will start streams only when there are files at least 4h old. The number of streams started is a function of the data volume, roughly starting one stream per 300GB"""
  # Check whether files are old enough to start something
  if ageOfOldestTapeCopy < 4*3600 :
    # Log input and decision
    f = open('/tmp/fileStream.dump','a')
    f.writelines("StreamPolicy called "+time.strftime("%d %b %Y %H:%M:%S", time.localtime())+" streamId="+str(streamId)+",numTapeCopies="+str(numTapeCopies)+",totalBytes="+str(totalBytes)+",ageOfOldestTapeCopy="+str(ageOfOldestTapeCopy)+",tapePoolId="+str(tapePoolId)+",tapePoolName="+tapePoolName+",nbRunningStreams="+str(nbRunningStreams)+",svcClassName="+svcClassName+" Files not old enough\n")
    f.close()
    return 0
    
  # Correction of 300 Mbytes per file due to the around 6 seconds spent writing labels and tape marks
  correctedDataVolume = totalBytes + (numTapeCopies*300000000)
  # Step function with 300 GB step
  wantedRunningStreams = int(correctedDataVolume/300000000000)
  # Start at least one stream
  if wantedRunningStreams == 0:
    wantedRunningStreams = 1
  # Check wanted streams against already running ones
  if wantedRunningStreams > nbRunningStreams:
    returncode = 1
  else:
    returncode = 0

  # log input and decision
  f = open('/tmp/fileStream.dump','a')
  f.writelines("StreamPolicy called "+time.strftime("%d %b %Y %H:%M:%S", time.localtime())+" streamId="+str(streamId)+",numTapeCopies="+str(numTapeCopies)+",totalBytes="+str(totalBytes)+",ageOfOldestTapeCopy="+str(ageOfOldestTapeCopy)+",tapePoolId="+str(tapePoolId)+",tapePoolName="+tapePoolName+",nbRunningStreams="+str(nbRunningStreams)+",svcClassName="+svcClassName+", wantedRunningStreams="+str(wantedRunningStreams)+", returncode="+str(returncode)+'\n')
  f.close()

  # return
  return returncode

# End-of-File
