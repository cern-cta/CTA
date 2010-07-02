/*
 * $Id: error.c,v 1.12 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2004 by CERN-IT
 * All rights reserved
 */

/* error.c      Remote File I/O - error numbers and message handling    */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include <Castor_limits.h>
#include "rfio.h"
#include <Cglobals.h>
#include <rfio_errno.h>

/*
 * RFIO global error number.
 */

char *rfio_lasthost() ;
char *rfio_serror() ;

/*
 * Get remote error string corresponding to code.
 */
char *rfio_errmsg_r (int     s,
                     int     code,
                     char  *buf,
                     size_t  buflen)
{
  char   *p;
  LONG   len;
  char msg[CA_MAXLINELEN+1];
  char rfio_buf[CA_MAXLINELEN+1];

  if( buf == NULL || buflen <=0 ) return NULL;
  memset(buf, '\0', buflen);

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_errmsg(%d, %d)",s,code) ;
  p= rfio_buf ;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_ERRMSG);
  marshall_LONG(p, code);
  TRACE(2,"rfio","rfio_errmsg: sending %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE)  {
    TRACE(2, "rfio" ,"rfio_errmsg: write(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return((char *) NULL);
  }
  TRACE(2, "rfio", "rfio_errmsg: reading %d bytes", LONGSIZE);
  if (netread_timeout(s,rfio_buf, LONGSIZE, RFIO_CTRL_TIMEOUT) != LONGSIZE) {
    TRACE(2, "rfio" ,"rfio_errmsg: read(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return((char *) NULL);
  }
  p = rfio_buf ;
  unmarshall_LONG(p, len);
  TRACE(2, "rfio", "rfio_errmsg: reading %d bytes", len);
  if (netread_timeout(s,rfio_buf,len,RFIO_CTRL_TIMEOUT) != len) {
    TRACE(2, "rfio" ,"rfio_errmsg: read(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return((char *) NULL);
  }
  p= rfio_buf;
  unmarshall_STRINGN(p, msg, CA_MAXLINELEN+1);
  TRACE(1, "rfio", "rfio_errmsg: <%s>", msg);
  END_TRACE();
  strcpy(buf, msg);
  return(buf);
}

static int rfio_error_key = -1;

char *rfio_errmsg(int     s,
                  int     code)
{
  char *buf = NULL;
  int buflen = CA_MAXLINELEN+1;

  Cglobals_get(&rfio_error_key, (void **)&buf, buflen);
  return(rfio_errmsg_r(s, code, buf, buflen));
}


/* print an error message  */
char *rfio_serror_r(char *buf,
                    size_t buflen)
{
  int          s;
  int  last_rferr ;  /* to preserve rfio_errno   */
  int  last_err ;  /* to preserve errno    */
  int          last_serrno ;   /* to preserve serrno                   */
  int   rt ;  /* Request is from other network?   */
  char  *rferrmsg ;
  char  rerrlist[CA_MAXLINELEN+1] ; /* Message from errlist   */

  if( buf == NULL || buflen <=0 ) return NULL;
  memset(buf, '\0', buflen);

  INIT_TRACE("RFIO_TRACE");
  last_err = errno ;
  last_rferr = rfio_errno ;
  last_serrno = serrno ;
  TRACE(2, "rfio", "rfio_serror: errno=%d, serrno=%d, rfio_errno=%d",
        errno, serrno, rfio_errno);
  END_TRACE();
  if (last_serrno != 0) {
    return (  sstrerror(serrno) );
  }
  else    {
    if (last_rferr != 0)        {
      if ((s=rfio_connect(rfio_lasthost(),&rt)) == -1)  {
        sprintf(rerrlist,"Unable to fetch remote error %d",last_rferr);
        rfio_errno = last_rferr ;
        strcpy(buf, rerrlist);
        return (buf);
      }
      else    {
        if ( (rferrmsg = rfio_errmsg(s,last_rferr)) != NULL )
          sprintf(rerrlist, "%s (error %d on %s)", rferrmsg, last_rferr, rfio_lasthost());
        else
          sprintf(rerrlist, " (error %d on %s)", last_rferr, rfio_lasthost());
        netclose(s);
        rfio_errno = last_rferr ;
        strcpy(buf, rerrlist);
        return (buf);
      }
    }
    else    {
      if (serrno != 0)  {
        strcpy( buf, sstrerror(serrno));
        return (buf);
      }
      else {
        strcpy(buf, strerror(last_err));
        return (buf);
      }
    }
  }
}

int rfio_serrno()   /* get error number - return -1 if cannot get it */
{
  int  last_rferr ;  /* to preserve rfio_errno   */
  int  last_err ;  /* to preserve errno    */
  int          last_serrno ;   /* to preserve serrno                   */

  INIT_TRACE("RFIO_TRACE");
  last_err = errno ;
  last_rferr = rfio_errno ;
  last_serrno = serrno ;
  TRACE(2, "rfio", "rfio_serrno: errno=%d, serrno=%d, rfio_errno=%d",
        errno, serrno, rfio_errno);
  END_TRACE();
  if (last_serrno != 0) {
    return (serrno);
  } else {
    if (last_rferr != 0) {
      rfio_errno = last_rferr ;
      return (rfio_errno);
    } else {
      if (serrno != 0)  {
        return (serrno);
      } else {
        return (last_err);
      }
    }
  }
}

static int rfio_serror_key = -1;

char *rfio_serror()
{
  char *buf = NULL;
  int buflen = CA_MAXLINELEN+1;

  Cglobals_get(&rfio_serror_key, (void **)&buf, buflen);
  return(rfio_serror_r(buf, buflen));
}


void rfio_perror(char *umsg)
{
  char *errmsg ;
  errmsg =  rfio_serror();
  if (errmsg != NULL )
    fprintf(stderr,"%s : %s\n",umsg, errmsg);
  else
    fprintf(stderr,"%s : No error message\n",umsg);
}
