######################################################################################
#
# CASTOR2 LSF Plugin - Sample Policy File
# $Id: scheduler.py,v 1.3 2009/07/23 12:22:03 waldron Exp $
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
#   by the rmmaster daemon with a predicted load added to the values whenever a file 
#   system is selected to run a job. When new monitoring information is received by the
#   rmmaster daemon the predicted values are reset to 0.

# General identifiers
Diskserver              = "" 
Filesystem              = "" 

# The total size of the file system in bytes, the total number of bytes free and the
# total number of file systems.
TotalSize               = 0
FreeSpace               = 0
TotalNbFilesystems      = 0

# The 'OpenFlags' can be one of [r/w/o] representing read, write and read write 
# directions respectively.
OpenFlags               = ""

# The following variables define how many streams of various types are running on a 
# given file system.
NbMigratorStreams       = 0
NbReadStreams           = 0
NbReadWriteStreams      = 0
NbRecallerStreams       = 0
NbWriteStreams          = 0

# The number of kilobytes being written too and read from the given file system per 
# second.
ReadRate                = 0
WriteRate               = 0

# The aggregate values for all file systems
TotalNbMigratorStreams  = 0
TotalNbReadStreams      = 0
TotalNbReadWriteStreams = 0
TotalNbRecallerStreams  = 0
TotalNbWriteStreams     = 0
TotalReadRate           = 0
TotalWriteRate          = 0

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
    weight -= NbReadStreams + NbReadWriteStreams + NbWriteStreams
	
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
    if OpenFlags == "w" or OpenFlags == "o":
	if NbMigratorStreams > 0 and NbWriteStreams + NbReadWriteStreams > 1:
	    return 1.0

    # Add some randomness and streams
    weight -= NbReadStreams + NbReadWriteStreams + NbWriteStreams
    weight -= random.random()

    return weight


# End-of-File

