/*
 * $Id: error.c,v 1.3 1999/12/09 08:46:58 baran Exp $
 *
 * $Log: error.c,v $
 * Revision 1.3  1999/12/09 08:46:58  baran
 * Thread-safe version
 *
 * Revision 1.2  1999/07/20 12:47:56  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990-1998 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)error.c	3.17 5/6/98 CERN CN-SW/DC Frederic Hemmer";
#endif /* not lint */

/* error.c      Remote File I/O - error numbers and message handling    */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1 
#include "rfio.h" 
#include <Cglobals.h>
#include <rfio_errno.h>

/*
 * RFIO global error number.
 */
/* int rfio_errno= 0 ; */

#ifndef linux 
extern char * sys_errlist[];
#endif


char *rfio_lasthost() ;
char *rfio_serror() ;

/*
 * Get remote error string corresponding to code.
 */
char *rfio_errmsg_r (s, code, buf, buflen)   
int     s;
int     code;
char 	*buf;
size_t 	buflen;
{
   char   *p;
   LONG   len;
   static char msg[256];
   char rfio_buf[256];

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
   unmarshall_STRING(p, msg);
   TRACE(1, "rfio", "rfio_errmsg: <%s>", msg);
   END_TRACE();
   strcpy(buf, msg);
   return(buf);
}

static int rfio_error_key = -1;

char *rfio_errmsg(s, code) 
int     s;
int     code;
{
   char *buf = NULL;
   int buflen = 256;

   Cglobals_get(&rfio_error_key, (void **)&buf, buflen);
   return(rfio_errmsg_r(s, code, buf, buflen));
}


char *rfio_serror_r(buf, buflen)                /* print an error message               	*/
char *buf;
size_t buflen;   
{
   int          s;
   int		last_rferr ; 	/* to preserve rfio_errno 		*/
   int		last_err ; 	/* to preserve errno 			*/
   int          last_serrno ;   /* to preserve serrno                   */
   int 		rt ;		/* Request is from other network ?  	*/
   char 	*rferrmsg ;
   static char rerrlist[256] ; 	/* Message from errlist */

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
	    strcpy(buf, sys_errlist[last_err]);
	    return (buf);
	 }
      }
   }
}

static int rfio_serror_key = -1;

char *rfio_serror() 
{
   char *buf = NULL;
   int buflen = 256;

   Cglobals_get(&rfio_serror_key, (void **)&buf, buflen);
   return(rfio_serror_r(buf, buflen));
}


void rfio_perror(umsg)
char *umsg ;
{
   char *errmsg ;
   errmsg =  rfio_serror();
   if (errmsg != NULL ) 
      fprintf(stderr,"%s : %s\n",umsg, errmsg);
   else
      fprintf(stderr,"%s : No error message\n",umsg);
}
