/*
 * $Id: rfstatfs.c,v 1.8 2009/06/03 13:57:06 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#define RFIO_KERNEL 1
#if !defined(_WIN32)
#include <sys/param.h>
#if defined(__APPLE__)
#include <sys/mount.h>
#endif
#if defined(linux)
#include <sys/vfs.h>
#endif
#endif
#include <rfio.h>

int DLL_DECL rfstatfs(path, statfsbuf )
     char *path ;
     struct rfstatfs *statfsbuf ;
{
  int status = 0   ;

#if defined(_WIN32)
  DWORD bps, nfc, spc, tnc;
  char *p, *rootpath;
  char pathbuf[256];
#else
  static struct statfs fsbuffer;
#endif
#if defined(linux) || defined(__APPLE__)
  if ( statfs(path,&fsbuffer) < 0 ) {
    status = -1;
  }
#endif
#if defined(_WIN32)
  if (*(path+1) == ':') {  /* drive name */
    pathbuf[0] = *path;
    pathbuf[1] = *(path+1);
    pathbuf[2] = '\\';
    pathbuf[3] = '\0';
    rootpath = pathbuf;
  } else if (*path == '\\' && *(path+1) == '\\') { /* UNC name */
    if ((p = strchr (path+2, '\\')) == 0)
      return (-1);
    if (p = strchr (p+1, '\\'))
      strncpy (pathbuf, path, p-path+1);
    else {
      strcpy (pathbuf, path);
      strcat (pathbuf, "\\");
    }
    rootpath = pathbuf;
  } else
    rootpath = NULL;
  if (GetDiskFreeSpace (rootpath, &spc, &bps, &nfc, &tnc) == 0) {
    status = -1;
  }
#endif

  /*
   * Affecting variables
   */

  if  ( status == 0 ) {
#if defined(linux) || defined(__APPLE__)
    statfsbuf->totblks = (long)fsbuffer.f_blocks;
    statfsbuf->freeblks = (long)fsbuffer.f_bavail;
    statfsbuf->totnods = (long)fsbuffer.f_files ;
    statfsbuf->freenods = (long)fsbuffer.f_ffree ;
    statfsbuf->bsize = (long)fsbuffer.f_bsize;
#endif
#if defined(_WIN32)
    statfsbuf->totblks = (long)tnc;
    statfsbuf->freeblks = (long)nfc;
    statfsbuf->totnods = (long)-1;
    statfsbuf->freenods = (long)-1;
    statfsbuf->bsize = (long)(spc * bps);
#endif
  }
  return status ;

}
