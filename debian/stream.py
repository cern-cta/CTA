##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                        Example For Stream Policy File                  #
#                                                                        #
##########################################################################

import time

def streamPolicyAlwaysReturning1(runningStreams,numFiles,dataVolume,maxNumStreams,age):
  return 1

def defaultStreamPolicy(runningStreams,numFiles,dataVolume,maxNumStreams,age):
  """the defaultStreamPolicy will start streams only when there are files at least 4h old. The number of streams started is a function of the data volume, roughly starting one stream per 300GB"""
  # check whether files are old enough to start something
  if age < 4*3600 :
    # log input and decision
    f = open('/tmp/fileStream.dump','a')
    f.writelines("StreamPolicy called "+time.strftime("%d %b %Y %H:%M:%S", time.localtime())+'\n'+"runningStreams "+str(runningStreams)+" numFiles "+str(numFiles)+" dataVolume "+str(dataVolume)+" maxNumStreams "+str(maxNumStreams)+ " age "+str(age)+" Files not old enough\n")
    f.close()
    return 0
    
  # Correction of 300 Mbytes per file due to the around 6 seconds spent writing labels and tape marks
  correctedDataVolume = dataVolume + (numFiles*300000000)
  # Step function with 300 GB step
  wantedRunningStreams = int(correctedDataVolume/300000000000)
  # dont go above the number of drives
  if wantedRunningStreams > maxNumStreams:
    wantedRunningStreams = maxNumStreams
  # and start at least one stream
  if wantedRunningStreams == 0:
    wantedRunningStreams = 1
  # check wanted streams against already running ones
  if wantedRunningStreams > runningStreams:
    returncode = 1
  else:
    returncode = 0

  # log input and decision
  f = open('/tmp/fileStream.dump','a')
  f.writelines("StreamPolicy called "+time.strftime("%d %b %Y %H:%M:%S", time.localtime())+'\n'+"runningStreams "+str(runningStreams)+" numFiles "+str(numFiles)+" dataVolume "+str(dataVolume)+" maxNumStreams "+str(maxNumStreams)+ " age "+str(age)+" wantedRunningStreams "+str(wantedRunningStreams)+" returncode "+str(returncode)+'\n')
  f.close()

  # return
  return returncode

# End-of-File
