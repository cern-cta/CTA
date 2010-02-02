##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                           Stream Policy for ATLAS                      #
#                                                                        #
##########################################################################

# Modules
import sys
import os
from time import time, localtime, strftime
from random import random

def steveStreamPolicy():
        print "Entered steveStreamPolicy"
        return 1

def defaultStreamPolicy(runningStreams,numFiles,dataVolume,maxNumStreams,age):
        return 1

def giveZeroSometimesOne(period,numberOfDrives):
	# To avoid indefinite postponenment, randomly,
        # once each "period" hours say yes, assuming mighunter period
        # to be 1 hour
        probability = 1./(period*numberOfDrives)
        uniform01pick = random()
        if probability > uniform01pick:
        	return 1
	else:
           	return 0

def firststreamPolicy(runningStreams,numFiles,dataVolume,maxNumStreams,age):

       	randomPolicy = 0
        # Correction of 300 Mbytes per file due to the around 6 seconds spent 
        # writing labels and tape marks
 	correctedDataVolume = dataVolume + (numFiles*300000000)
        # Step function with 300 GB step
        wantedRunningStreams = int(correctedDataVolume/300000000000)
        # dont go above the number of drives
	if wantedRunningStreams > maxNumStreams:
        	wantedRunningStreams = maxNumStreams

	if wantedRunningStreams > runningStreams:
               	myreturncode = 1
        else:
        	randomPolicy = 1
		myreturncode = giveZeroSometimesOne(4,maxNumStreams)

        f = open('/tmp/fileStream.dump','a')
        f.writelines("StreamPolicy called "+strftime("%d %b %Y %H:%M:%S", localtime())+' '+"runningStreams "+str(runningStreams)+" numFiles "+str(numFiles)+" dataVolume "+str(dataVolume)+" nbDrives "+str(maxNumStreams)+" wantedRunningStreams "+str(wantedRunningStreams)+" returncode "+str(myreturncode))
	if randomPolicy == 1:
		f.writelines(" random");
	else:
		f.writelines(" determ");
	f.writelines('\n');
        f.close()

	return myreturncode

# End-of-File
