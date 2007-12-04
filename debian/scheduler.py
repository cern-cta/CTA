######################################################################################
#
# CASTOR2 LSF Plugin - Sample Policy File
# $Id: scheduler.py,v 1.1 2007/12/04 12:43:12 waldron Exp $
#
######################################################################################

# Modules
import sys
import random

# The following file defines the policies that the CASTOR LSF plugin uses to select 
# the best file system to schedule a job on. The plugin via the use of an embedded 
# Python interpreter loads that policies at start-up and will attempt to call a 
# function in the modules namespace that matches the name of svcclass or diskpool where
# the job is required to run. Should the function not exist the default will be taken.
#
# After modification of this file, the policies must be reloaded into the plugin using 
# `badmin reconfig` on the LSF master.

# In order to select the best location in terms of a disk server and file system, the 
# plugin sets a number of global variables to be used with the various functions to 
# help the selection process.
#
# Note:
#   These variables are not a true reflection of the state of a disk server and is its 
#   file system(s) but an approximation based on the monitoring information collected 
#   by the rmMasterDaemon with a predicated load added to the values whenever a file 
#   system is selected to run a job. When new monitoring information is received by the
#   rmMasterDaemon the predicated values are reset to 0.

# General identifiers
key                   = "" # The LSF job name, aka subrequest id
hostname              = "" 
filesystem            = "" 

# The total number of LSF job slots allocated to the machine
slotsAlloc            = 0

# The size of the file to be scheduled and the direction of its associated stream. The 
# 'filesDirection' can be one of [r/w/o] representing read, write and read write 
# directions respectively. 
#
# Note:
#   A file size of 0 is normal for jobs which are classified as reads.
#
fileDirection         = ""
fileSize              = 0

# The following variables define how many streams of various types are running on a 
# given file system.
migratorStreams       = 0
readStreams           = 0
readWriteStreams      = 0
recallerStreams       = 0
writeStreams          = 0

# The number of kilobytes being written too and read from the given filesystem per 
# second.
readRate              = 0
writeRate             = 0

# The total size of the file system in bytes and the total number of bytes free.
totalSize             = 0
freeSpace             = 0

# The aggregate values for all file systems
totalMigratorStreams  = 0
totalReadStreams      = 0
totalReadWriteStreams = 0
totalRecallerStreams  = 0
totalWriteStreams     = 0

totalFileSystems      = 0
totalReadRate         = 0
totalWriteRate        = 0

#
# All functions in the module matching the same name as a svcclass or diskpool must 
# return a value of type float. Where a positive value means the file system should be 
# excluded from the possible list of candidates where the job could run. A negative 
# value represents the weighting of the file system. The file system at the end of the 
# selection process with the highest weighting will be selected to run the job.
#
# For example, consider the following disk server and file system weights
#
#  lxfsrk5701.cern.ch:/srv/castor/01	1.0
#  lxfsrk5701.cern.ch:/srv/castor/02   -5.0
#  lxfsrk5701.cern.ch:/srv/castor/03    1.0
#  lxfsrk5901.cern.ch:/srv/castor/01   -2.0
#  lxfsrk5901.cern.ch:/srv/castor/02   -3.3
#  lxfsrk5901.cern.ch:/srv/castor/03   -4.1
#
# File systems 01 and 03 on disk server lxfsrk5701 will be rejected as candidates to 
# run the job. The winning disk server will be lxfsrk5901 with file system 
# /srv/castor/01 as it has the highest weight.
#
# Note:
#   The svcclass/diskpool functions are called for each candidate disk server and file 
#   system individually

# Default:
#
# A simple selection policy which only takes into account the number of streams on a 
# file system
#
def default():
    weight  = 0.0
    weight -= readStreams + readWriteStreams + writeStreams
	
    # We add a little bit of randomness to the weight to given us a better distribution
    # across disk servers	
    weight -= random.random()

    # If the randomness is excluded above then it is possible that the weight value is 
    # 0.0. This is a positive value and will result in the file system being excluded. 
    # Not what we want here!

    return weight


# Export:
#
# A policy that gives preference to migrators on a file system by limiting the number 
# of writers to the same file system as writing has higher priority then reading at 
# the kernel level
#
def export():
    weight = 0.0
		
    # If the direction of the stream is a write or readWrite check to see if any 
    # migrators are present. If so, restrict the number of writers on the file system 
    # to 1
    if fileDirection == "w" or fileDirection == "o":
	if migratorStreams > 0 and writeStreams + readWriteStreams > 1:
	    return 1.0

    # Add some randomness and streams
    weight -= readStreams + readWriteStreams + writeStreams
    weight -= random.random()

    return weight


# End-of-File

