/*
 * $Id: readlink.c,v 1.7 2000/12/21 15:21:54 jdurand Exp $
 */


/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: readlink.c,v $ $Revision: 1.7 $ $Date: 2000/12/21 15:21:54 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

#define RFIO_KERNEL     1
#include "rfio.h"

#ifndef linux
extern char *sys_errlist[];     /* system error list */
#endif

int DLL_DECL rfio_readlink(path,buf, length)
char *path ;
char *buf ;
int length ;

{
   int c;
   int status ;
   int s ;
   char *host ;
   char * filename;
   char *p ;
   int rt ;
   int rcode , len;
   int uid ;
   int gid ;
   char buffer[MAXFILENAMSIZE];
   char tmpbuf[MAXFILENAMSIZE];

   /*
    * The file is local.
    */
   INIT_TRACE("RFIO_TRACE");
   TRACE( 1, "rfio", " rfio_readlink (%s,%x,%d)",path,buf,length);
   if ( ! rfio_parseln(path,&host,&filename,NORDLINKS) ) {
#if !defined(_WIN32)
      status = readlink(filename,buf,length) ;
#else
      { serrno = SEOPNOTSUP; status = -1; }
#endif
      END_TRACE() ;
      rfio_errno = 0;
      return status ;
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
      unmarshall_STRING( p, tmpbuf ) ;
      memcpy (buf, tmpbuf, length) ;
   }
   TRACE (2,"rfio","rfio_readlink succeded: returned %s",buf);
   END_TRACE();
   (void) close (s) ;
   return(rcode) ;
}

