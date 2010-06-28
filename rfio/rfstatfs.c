/*
 * $Id: rfstatfs.c,v 1.8 2009/06/03 13:57:06 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#define RFIO_KERNEL 1
#include <sys/param.h>
#if defined(__APPLE__)
#include <sys/mount.h>
#endif
#if defined(linux)
#include <sys/vfs.h>
#endif
#include <rfio.h>

int DLL_DECL rfstatfs(path, statfsbuf )
     char *path ;
     struct rfstatfs *statfsbuf ;
{
  int status = 0   ;

  static struct statfs fsbuffer;
  if ( statfs(path,&fsbuffer) < 0 ) {
    status = -1;
  }

  /*
   * Affecting variables
   */

  if  ( status == 0 ) {
    statfsbuf->totblks = (long)fsbuffer.f_blocks;
    statfsbuf->freeblks = (long)fsbuffer.f_bavail;
    statfsbuf->totnods = (long)fsbuffer.f_files ;
    statfsbuf->freenods = (long)fsbuffer.f_ffree ;
    statfsbuf->bsize = (long)fsbuffer.f_bsize;
  }
  return status ;

}
