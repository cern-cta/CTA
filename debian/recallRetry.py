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

def recallRetry(errorCode,nbRetry):
   return 1

#def recallRetry(errorCode,nbRetry):
#
#  # no retry
#  
#  if errorCode == ENOENT:
#     return 0
#  if errorCode == EACCESS:
#     return 0
#  if errorCode == EFAULT:
#     return 0
#  if errorCode == SENOSHOST:
#     return 0
#  if errorCode == SENOSSERV:
#     return 0
#  if errorCode == ENSFILECHG: 
#     return 0
#  if errorCode == ENOTDIR:
#     return 0
#  if errorCode == EISDIR: 
#     return 0
#  if errorCode == ENAMETOOLONG: 
#     return 0
#  if errorCode == EPERM: 
#     return 0
#  if errorCode == SEENTRYNFND:
#     return 0
#  if errorCode == SENOMAPFND:   
#     return 0
#  if errorCode == SECHECKSUM: 
#     return 0
#
#  # 5 retries
#  
#  if errorCode == EINVAL and nbRetry >=  5:
#     return 0
#  if errorCode == SECOMERR and nbRetry >=  5:
#     return 0   
#  if errorCode == EVMGRNACT and nbRetry >=  5:
#     return 0 
#  if errorCode == ENSNACT and nbRetry >=  5:
#     return 0
#  if errorCode == ENOMEM and nbRetry >=  5:
#     return 0 
#  if errorCode == ERTZEROSIZE and nbRetry >=  5:
#     return 0
#  if errorCode == ERTWRONGSIZE and nbRetry >=  5:
#     return 0  
#  if errorCode == ERTWRONGFSEQ and nbRetry >=  5:
#     return 0
#
#  # 1000 retries
#
#  if errorCode == EEXIST  and nbRetry >=  1000:
#     return 0
#
#  # other cases we loop
#  return 1
