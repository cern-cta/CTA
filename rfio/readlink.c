/*
 * $Id: readlink.c,v 1.13 2008/07/31 07:09:13 sponcec3 Exp $
 */


/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#define RFIO_KERNEL     1
#include "rfio.h"
#include <string.h>
#include <unistd.h>

int rfio_readlink(char *path,
                  char *buf,
                  int length)
{
  int c;
  int status ;
  int s ;
  char *host ;
  char * filename;
  char *p ;
  int rt ;
  int rcode , len, parserc;
  int uid ;
  int gid ;
  char buffer[BUFSIZ];
  char tmpbuf[MAXFILENAMSIZE];

  /*
   * The file is local.
   */
  INIT_TRACE("RFIO_TRACE");
  TRACE( 1, "rfio", " rfio_readlink (%s,%x,%d)",path,buf,length);
  if ( ! (parserc = rfio_parseln(path,&host,&filename,NORDLINKS)) ) {
    status = readlink(filename,buf,length) ;
    if ( status < 0 ) serrno = 0;
    END_TRACE() ;
    rfio_errno = 0;
    return status ;
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
  uid = geteuid() ;
  gid = getegid () ;

  p = buffer ;
  marshall_WORD(p, B_RFIO_MAGIC);
  marshall_WORD(p, RQST_READLINK);

  status = 2*WORDSIZE + strlen(path) + 1;
  marshall_LONG(p, status) ;
  if ( status > BUFSIZ ) {
    TRACE(2,"rfio","rfio_readlink: request too long %d (max %d)",
          status,BUFSIZ);
    END_TRACE();
    (void) close(s);
    serrno = E2BIG;
    return(-1);
  }

  if (netwrite_timeout(s,buffer,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio", "readlink: write(): ERROR occured (errno=%d)",errno);
    (void) close(s);
    END_TRACE();
    return(-1);
  }

  p = buffer ;
  marshall_WORD(p,uid) ;
  marshall_WORD(p,gid) ;
  marshall_STRING(p,filename) ;

  if (netwrite_timeout(s,buffer,status,RFIO_CTRL_TIMEOUT) != status ) {
    TRACE(2, "rfio", "readlink(): write(): ERROR occured (errno=%d)",errno);
    (void) close(s);
    END_TRACE();
    return(-1);
  }

  /*
   * Getting back status
   */
  if ((c=netread_timeout(s, buffer, 3*LONGSIZE, RFIO_CTRL_TIMEOUT)) != (3*LONGSIZE))  {
    if (c == 0) {
      serrno = SEOPNOTSUP;    /* symbolic links not supported on remote machine */
      TRACE(2, "rfio", "rfio_readlink: read(): ERROR occured (serrno=%d)", serrno);
    } else
      TRACE(2, "rfio", "rfio_readlink: read(): ERROR occured (errno=%d)", errno);
    (void) close(s);
    END_TRACE();
    return(-1);
  }
  p = buffer;
  unmarshall_LONG( p, len );
  unmarshall_LONG( p, status ) ;
  unmarshall_LONG( p, rcode ) ;

  if ( status < 0 ) {
    TRACE(1,"rfio","rfio_readlink(): rcode = %d , status = %d",rcode, status);
    rfio_errno = rcode ;
    (void) close(s);
    END_TRACE();
    return(status);
  }
  /* Length is not of a long size, so RFIO_CTRL_TIMEOUT is enough */
  if (netread_timeout(s, buffer, len, RFIO_CTRL_TIMEOUT) != len)  {
    TRACE(2, "rfio", "rfio_readlink: read(): ERROR occured (errno=%d)", errno);
    (void) close(s);
    END_TRACE();
    return(-1);
  }
  p = buffer ;
  if (rcode < length) {
    unmarshall_STRING( p, buf ) ;
  } else {
    unmarshall_STRINGN( p, tmpbuf, MAXFILENAMSIZE) ;
    memcpy (buf, tmpbuf, length) ;
  }
  TRACE (2,"rfio","rfio_readlink succeded: returned %s",buf);
  END_TRACE();
  (void) close (s) ;
  return(rcode) ;
}
