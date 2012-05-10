# Modules
import sys
import os

ENOENT = 2

# Default number of retries

defaultNormalRetries = 7

def migrationRetry(errorCode,nbRetry):
   # When a file is not present anymore in the name server, we get
   # this error code after a successful migration of the now-obsolete
   # file. There is no point in retrying, so we fail immediately.
   # The diskcopy will remain in the stager until the garbage
   # collector picks it up.
   if errorCode == ENOENT:
      return 0

   if nbRetry >= defaultNormalRetries:
      return 0
  
   return 1
