/*
 * $Id: statfs.c,v 1.4 1999/12/09 13:47:18 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: statfs.c,v $ $Revision: 1.4 $ $Date: 1999/12/09 13:47:18 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* statfs.c       Remote File I/O - get file system status     */

#define RFIO_KERNEL 1
#include "rfio.h"               /* Remote File I/O general definitions  */

int  rfio_statfs(path, statfsbuf)  
char    *path;              	/* remote file path                     */
struct rfstatfs *statfsbuf;     /* status buffer (subset of local used) */
{
   static char     buf[256];       /* General input/output buffer          */
   register int    s;      /* socket descriptor            */
   int      status;
   int      len;
   char     *host, *filename;
   char     *p=buf;
   int 	 rt ;
   int	 rcode ;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_statfs(%s, %x)", path, statfsbuf);

   if (!rfio_parse(path,&host,&filename)) {
      /* if not a remote file, must be local  */
      TRACE(1, "rfio", "rfio_statfs:  using local statfs(%s, %x)",
	    filename, statfsbuf);

      END_TRACE();
      rfio_errno = 0;
      return(rfstatfs(filename , statfsbuf));
   }

   s = rfio_connect(host,&rt);
   if (s < 0)      {
      END_TRACE();
      return(-1);
   }

   len = strlen(path)+1;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_STATFS);
   marshall_LONG(p, len);
   p= buf + RQSTSIZE;
   marshall_STRING(p, filename);
   TRACE(2,"rfio","rfio_statfs: sending %d bytes",RQSTSIZE+len) ;
   if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      TRACE(2, "rfio", "rfio_statfs: write(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   p = buf;
   TRACE(2, "rfio", "rfio_statfs: reading %d bytes", 7*LONGSIZE);
   if (netread_timeout(s, buf, 7*LONGSIZE, RFIO_CTRL_TIMEOUT) != (7*LONGSIZE))  {
      TRACE(2, "rfio", "rfio_statfs: read(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   unmarshall_LONG( p, statfsbuf->bsize ) ;
   unmarshall_LONG( p, statfsbuf->totblks ) ;
   unmarshall_LONG( p, statfsbuf->freeblks ) ;
   unmarshall_LONG( p, statfsbuf->totnods ) ;
   unmarshall_LONG( p, statfsbuf->freenods ) ;
   unmarshall_LONG( p, status ) ;
   unmarshall_LONG( p, rcode ) ;

   TRACE(1, "rfio", "rfio_statfs: return %d",status);
   rfio_errno = rcode ;
   (void) close(s);
   END_TRACE();
   return (status);
}

