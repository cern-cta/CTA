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
  int status ;  /* Status and return code from remote   */
  int rcode;
  int req;
  int s;
  int s_index;
  int namlen;
  struct dirent *de;
  char *p;
  extern RDIR *rdirfdt[MAXRFD];
  char     rfio_buf[BUFSIZ];

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

  s = rdirfdt[s_index]->s;

  /*
   * Associate the static dirent area allocate with this
   * directory descriptor.
   */

  de = (struct dirent *)rdirfdt[s_index]->dp.dd_buf;

  /*
   * Checking magic number.
   */
  if (rdirfdt[s_index]->magic != RFIO_MAGIC) {
    serrno = SEBADVERSION ;
    rfio_rdirfdt_freeentry(s_index);
    (void) close(s) ;
    END_TRACE();
    return(NULL);
  }
  p = rfio_buf;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_READDIR) ;
  TRACE(2,"rfio","rfio_readdir: writing %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE)  {
    TRACE(2,"rfio","rfio_readdir: write(): ERROR occured (errno=%d)", errno) ;
    END_TRACE() ;
    return(NULL) ;
  }
  TRACE(2, "rfio", "rfio_readdir: reading %d bytes",WORDSIZE+3*LONGSIZE) ;
  if ( netread_timeout(s,rfio_buf,WORDSIZE+3*LONGSIZE,RFIO_CTRL_TIMEOUT) != (WORDSIZE+3*LONGSIZE) ) {
    TRACE(2,"rfio","rfio_readdir: read(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return(NULL) ;
  }
  p= rfio_buf ;
  unmarshall_WORD(p,req) ; /* RQST_READDIR */
  unmarshall_LONG(p,status) ;
  unmarshall_LONG(p, rcode) ;
  unmarshall_LONG(p,namlen) ;
  if ( status < 0 ) {
    rfio_errno= rcode ;
    if ( rcode == 0 )
      serrno = SENORCODE ;
    END_TRACE() ;
    return(NULL) ;
  }
  if ( namlen > 0 ) {
    TRACE(2, "rfio", "rfio_readdir: reading %d bytes",namlen) ;
    memset(de->d_name,'\0',256);
    /* Directory name is of a small size, so I put RFIO_CTRL_TIMEOUT instead */
    /* of RFIO_DATA_TIMEOUT */
    if ( netread_timeout(s,de->d_name,namlen,RFIO_CTRL_TIMEOUT) != namlen ) {
      TRACE(2,"rfio","rfio_readdir: read(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return(NULL) ;
    }
    /*
     * Update the directory offset.
     */
    dirp->offset++;
    dirp->dp.dd_loc = dirp->offset;
    de->d_reclen = sizeof(struct dirent) + namlen;
#if !defined(linux)
    de->d_namlen = namlen;
#endif
  } else {
    TRACE(2,"rfio","rfio_readdir: no more directory entries");
    END_TRACE();
    return(NULL);
  }
  END_TRACE();
  return(de);
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
