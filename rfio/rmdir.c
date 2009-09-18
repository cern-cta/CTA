/*
 * $Id: rmdir.c,v 1.14 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 1998-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* rmdir.c       Remote File I/O - make a directory file                */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */


int  DLL_DECL rfio_rmdir(dirpath)           /* Remote rmdir             */
     char  *dirpath;          /* remote directory path             */
{
  static char     buf[BUFSIZ];       /* General input/output buffer          */
  register int    s;              /* socket descriptor            */
  int             status;         /* remote rmdir() status        */
  int      len;
  char     *host,
    *filename;
  char     *p=buf;
  int   rt ;
  int   rcode, parserc ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_rmdir(%s)", dirpath);

  if (!(parserc = rfio_parseln(dirpath,&host,&filename,NORDLINKS))) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      TRACE(1,"rfio","rfio_rmdir: %s is an HSM path %s",
            dirpath,host);
      END_TRACE();
      rfio_errno = 0;
      // This check is needed to support relative directory pathes
      // See the implementation of rfio_HsmIf_rmdir and of the
      // rfio_HsmIf_IsCnsFile method for details. In brief, if
      // a relative directory path is given and rfchdir was used
      // before that, rmdir will be able to work. If no chdir was
      // called, rmdir will fail
      if (filename && (strstr(filename,"/castor")==filename))
        return rfio_HsmIf_rmdir(filename);
      return(rfio_HsmIf_rmdir(dirpath));
    }

    TRACE(1, "rfio", "rfio_rmdir: using local rmdir(%s)",
          filename);

    END_TRACE();
    rfio_errno = 0;
    return(rmdir(filename));
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

  len = strlen(filename) + 1;
  if ( RQSTSIZE+len > BUFSIZ ) {
    TRACE(2,"rfio","rfio_rmdir: request too long %d (max %d)",
          RQSTSIZE+len,BUFSIZ);
    END_TRACE();
    (void) netclose(s);
    serrno = E2BIG;
    return(-1);
  }
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_RMDIR);
  marshall_WORD(p, geteuid());
  marshall_WORD(p, getegid());
  marshall_LONG(p, len);
  p= buf + RQSTSIZE;
  marshall_STRING(p, filename);
  TRACE(2,"rfio","rfio_rmdir: sending %d bytes",RQSTSIZE+len) ;
  if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
    TRACE(2, "rfio", "rfio_rmdir: write(): ERROR occured (errno=%d)", errno);
    (void) close(s);
    END_TRACE();
    return(-1);
  }
  p = buf;
  TRACE(2, "rfio", "rfio_rmdir: reading %d bytes", LONGSIZE);
  if (netread_timeout(s, buf, 2* LONGSIZE, RFIO_CTRL_TIMEOUT) != (2 * LONGSIZE))  {
    TRACE(2, "rfio", "rfio_rmdir: read(): ERROR occured (errno=%d)", errno);
    (void) close(s);
    END_TRACE();
    return(-1);
  }
  unmarshall_LONG(p, status);
  unmarshall_LONG(p, rcode);
  TRACE(1, "rfio", "rfio_rmdir: return %d",status);
  rfio_errno = rcode;
  (void) close(s);
  if (status)     {
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return (0);
}
