# Modules
import sys
import os

# error codes

ENOENT = 2
EACCESS = 13
EFAULT = 14
EINVAL = 22
SENOSHOST = 1001
SENOSSERV = 1002
SETIMEDOUT = 1004
SECOMERR = 1018
EVMGRNACT = 2001
ENSFILECHG = 1402
ENOTDIR = 20
EISDIR = 21
ENAMETOOLONG = 1008
ENSNACT = 1401
ENOMEM = 12
EPERM = 1
SEENTRYNFND = 1014
SENOMAPFND = 1020
EBADF = 9
SECHECKSUM = 1037 
EEXIST = 17
ERTZEROSIZE = 1612
ERTWRONGSIZE = 1613
ERTWRONGFSEQ = 1614

# Default number of retries

defaultNormalRetries = 5
defaultMaxRetries    = 1000

def migrationRetry(errorCode,nbRetry):
   # When a file is not present anymore in the name server, we get
   # this error code after a successful migration of the now-obsolete
   # file. There is no point in retrying, so we fail immediately.
   # The diskcopy will remain in the stager until the garbage
   # collector picks it up.
   if errorCode == ENOENT:
      return 0

   if errorCode == SETIMEDOUT and nbRetry >= defaultNormalRetries:
      return 0

   if errorCode == SECHECKSUM and nbRetry >= defaultNormalRetries:
      return 0
  
   # Default strategy is to retry indefinitely.
   return 1
