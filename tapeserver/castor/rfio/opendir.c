/*
 * $Id: opendir.c,v 1.22 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* opendir.c       Remote File I/O - open a directory                   */
#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include <syslog.h>             /* system logger                        */
#include <pwd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "rfio.h"               /* remote file I/O definitions          */
#include "rfio_rdirfdt.h"
#include "rfcntl.h"             /* remote file control mapping macros   */
#include <arpa/inet.h>          /* for inet_ntoa()                      */
#include <Cpwd.h>
extern char *getacct();

RDIR  *rdirfdt[MAXRFD];         /* File descriptors tables             */

/*
 * Forward declaration
 */
RDIR *rfio_opendir_ext();

RDIR *rfio_opendir(char *dirpath)
{
  char rh[1];
  rh[0] = '\0';
  return(rfio_opendir_ext(dirpath,(uid_t)0,(gid_t)0,0,rh,rh));
}

RDIR *rfio_opendir_ext(char *dirpath,
                       uid_t uid,
                       gid_t gid,
                       int passwd,
                       char  * reqhost, /* In case of a Non-mapped I/O with uid & gid
					   sepcified, which host will be contacted
					   for key check ? */
                       char   *vmstr)
{
  char    * host ;
  char  *dirname ;
  RDIR      * dp ;      /* Local directory pointer      */
  int  parserc ;  /* daemon in site(0) or not (1) */

  INIT_TRACE("RFIO_TRACE");
  TRACE(1,"rfio","rfio_opendir(%s)",dirpath);
  /*
   * The directory is local.
   */
  host = NULL;
  if ( ! (parserc = rfio_parse(dirpath,&host,&dirname)) ) {
    /* if not a remote file, must be local or HSM  */
    TRACE(2,"rfio","rfio_opendir(%s) rfio_parse returns host=%s",
          dirpath,(host != NULL ? host : "(nil)"));
    if ( host != NULL ) {
      /*
       * HSM file
       */
      rfio_errno = 0;
      dp = (RDIR *)rfio_HsmIf_opendir(dirname);
    } else {
      dp = (RDIR *)opendir(dirname);
      if ( ! dp ) serrno = 0;
    }
    END_TRACE() ;
    return(dp);
  }
  if (parserc < 0) {
    END_TRACE();
    return(NULL);
  }

  // Remote directory. Not supported anymore
  TRACE(1,"rfio","rfio_opendir: rcode(%d)", SEOPNOTSUP) ;
  rfio_errno = SEOPNOTSUP;
  /* Operation failed but no error message was sent */
  END_TRACE() ;
  return(NULL);
}
