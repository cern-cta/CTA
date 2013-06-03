/*
 * $Id: rtcpd.c,v 1.8 2009/08/18 09:43:01 waldron Exp $
 *
 * Copyright (C) 1999-2012 by CERN IT
 * All rights reserved
 */

/*
** An xroot wrapper for file commands used by rtcpd.
** We need it to be able to use any compilation flags specific only for xroot
*/ 

#include <rtcp_xroot.h>

/* XrdPosixExtern.hh requires the following three compilation options */
#define _LARGEFILE_SOURCE
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <XrdPosix/XrdPosixExtern.hh>

int        rtcp_xroot_stat(const char *path, struct stat *buf) {
  return XrdPosix_Stat(path,buf);
}

int        rtcp_xroot_open(const char *path, int oflag, int mode) {
  return XrdPosix_Open(path, oflag, mode);
}

long long  rtcp_xroot_lseek(int fildes, long long offset, int whence) {
  return XrdPosix_Lseek(fildes, offset, whence);
}

int        rtcp_xroot_close(int fildes) {
  return XrdPosix_Close(fildes);
}

long long  rtcp_xroot_write(int fildes, const void *buf, 
                            unsigned long long nbyte) {
  return XrdPosix_Write(fildes, buf, nbyte);
}

long long  rtcp_xroot_read(int fildes, void *buf, unsigned long long nbyte) {
  return XrdPosix_Read(fildes, buf, nbyte);
}


