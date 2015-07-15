/*
 * $Id: readdir.c,v 1.16 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* readdir.c       Remote File I/O - read  a directory entry            */

#include <syslog.h>             /* system logger    */
#include <string.h>

/*
 * System remote file I/O definitions
 */

#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rdirfdt.h"

/*
 * Remote directory read
 */

struct dirent *rfio_readdir(dirp)
     RDIR *dirp;
{
  int s_index;
  struct dirent *de;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_readdir(%x)", dirp);

  /*
   * Search internal table for this directory pointer
   * Check first that it's not the last one used
   */
  s_index = rfio_rdirfdt_findptr(dirp,FINDRDIR_WITH_SCAN);

  /*
   * The directory is local.
   */
  if (s_index == -1) {
    TRACE(2,"rfio","rfio_readdir: check if HSM directory");
    if ( rfio_HsmIf_IsHsmDirEntry((DIR *)dirp) != -1 ) {
      de = rfio_HsmIf_readdir((DIR *)dirp);
    } else {
      TRACE(2,"rfio","rfio_readdir: using local readdir(%x)", dirp);
      de = readdir((DIR *)dirp);
      if ( ! de ) serrno = 0;
    }
    END_TRACE();
    return(de);
  }

  // Remote directory. Not supported anymore
  rfio_errno = SEOPNOTSUP;
  END_TRACE() ;
  return(NULL) ;
}

#if !defined(linux)
struct dirent *rfio_readdir64(dirp)
     RDIR *dirp;
{
  return (rfio_readdir(dirp));
}
#else
struct dirent64 *rfio_readdir64(dirp)
     RDIR *dirp;
{
  struct dirent64 *de;
  struct dirent *de32;
  ino_t ino;
  short namlen;
  off_t offset;

  if ((de32 = rfio_readdir(dirp)) == NULL)
    return(NULL);

  ino = de32->d_ino;
  offset = de32->d_off;
  namlen = strlen(de32->d_name);
  de = (struct dirent64 *) de32;
  memmove (de->d_name, de32->d_name, namlen + 1);
  de->d_ino = ino;
  de->d_off = offset;
  de->d_reclen = ((&de->d_name[0] - (char *) de + namlen + 8) / 8) * 8;
  return(de);
}
#endif
