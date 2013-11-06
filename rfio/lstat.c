/*
 * $Id: lstat.c,v 1.20 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* lstat.c       Remote File I/O - get file status   */

#define RFIO_KERNEL     1
#include <pwd.h>
#include "rfio.h"
#include <Cglobals.h>
#include <Cpwd.h>
#include <string.h>
#include <unistd.h>

/* Remote file lstat */
int rfio_lstat(char    *filepath,               /* remote file path    */
               struct stat *statbuf)            /* status buffer   */
{
  int      lstatus;  /* remote lstat() status     */
  struct stat64 statb64;

  if ((lstatus = rfio_lstat64(filepath,&statb64)) == 0)
    (void) stat64tostat(&statb64, statbuf);
  return (lstatus);
}

/* Remote file lstat    */
int rfio_lstat64(char    *filepath,                              /* remote file path     */
                 struct stat64 *statbuf)                         /* status buffer        */
{
  register int    s;                           /* socket descriptor    */
  int       status ;
  char     *host, *filename;
  int      rt, parserc ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_lstat64(%s, %x)", filepath, statbuf);

  if (!(parserc = rfio_parseln(filepath,&host,&filename,NORDLINKS))) {
    /* if not a remote file, must be local or HSM                     */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      TRACE(1,"rfio","rfio_lstat64: %s is an HSM path", filename);
      END_TRACE();
      rfio_errno = 0;
      return(rfio_HsmIf_stat64(filename,statbuf));
    }
    TRACE(1, "rfio", "rfio_lstat64: using local lstat64(%s, %x)",
          filename, statbuf);

    END_TRACE();
    rfio_errno = 0;
    status = lstat64(filename,statbuf);
    if ( status < 0 ) serrno = 0;
    return(status);
  }
  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  s = rfio_connect(host,&rt);
  if (s < 0)      {
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  status = rfio_smstat64(s, filename, statbuf, RQST_LSTAT64) ;
  if ( status == -1 && serrno == SEPROTONOTSUP ) {
    /* The RQST_LSTAT64 is not supported by remote server          */
    /* So, we use RQST_LSTAT_SEC instead                           */
    s = rfio_connect(host,&rt);    /* First: reconnect             */
    if (s < 0)      {
      return(-1);
    }
    status = rfio_smstat64(s, filename, statbuf, RQST_LSTAT_SEC);
    if ( status == -1 && serrno == SEPROTONOTSUP ) {
      /* The RQST_LSTAT_SEC is not supported by remote server     */
      /* So, we use RQST_LSTAT instead                            */
      s = rfio_connect(host,&rt);
      if (s < 0)      {
        return(-1);
      }
      status = rfio_smstat64(s, filename, statbuf, RQST_LSTAT) ;
    }
  }
  (void) close(s);
  return (status);
}
