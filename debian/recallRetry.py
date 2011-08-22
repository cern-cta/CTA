# This recall-retry policy file is used by tapegewayd.
#
# This recall-retry policy implements the behaviour of retrying any given file
# recall no more than 2 times and immediately failing error scenarios that are
# known to be hopeless.

# Modules
import sys
import os

# error codes

EPERM        =    1 
ENOENT       =    2 
EBADF        =    9   
ENOMEM       =   12
EACCESS      =   13
EFAULT       =   14
EEXIST       =   17 
ENOTDIR      =   20
EISDIR       =   21 
EINVAL       =   22
SENOSHOST    = 1001
SENOSSERV    = 1002   
ENAMETOOLONG = 1008 
SECOMERR     = 1018   
ENSNACT      = 1401 
ENSFILECHG   = 1402
SEENTRYNFND  = 1014 
SENOMAPFND   = 1020  
SECHECKSUM   = 1037 
ERTZEROSIZE  = 1612 
ERTWRONGSIZE = 1613
ERTWRONGFSEQ = 1614   
ETHWERR      = 1916 # device malfunction
ETPARIT      = 1917 # parity error
EVMGRNACT    = 2001   

#def recallRetry(errorCode,nbRetry):
#   return 1

def recallRetry(errorCode,nbRetry):

  # Cases which should never be retried
  if errorCode == ENOENT:
     return 0
  if errorCode == EACCESS:
     return 0
  if errorCode == EFAULT:
     return 0
  if errorCode == SENOSHOST:
     return 0
  if errorCode == SENOSSERV:
     return 0
  if errorCode == ENSFILECHG: 
     return 0
  if errorCode == ENOTDIR:
     return 0
  if errorCode == EISDIR: 
     return 0
  if errorCode == ENAMETOOLONG: 
     return 0
  if errorCode == EPERM: 
     return 0
  if errorCode == SEENTRYNFND:
     return 0
  if errorCode == SENOMAPFND:   
     return 0
  if errorCode == SECHECKSUM: 
     return 0

  # Cases that can be retried get 2 attempts
  if nbRetry >= 2:
     return 0

  return 1
