/*
 * $Id: unlink.c,v 1.4 2004/01/23 10:27:46 jdurand Exp $
 */


/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: unlink.c,v $ $Revision: 1.4 $ $Date: 2004/01/23 10:27:46 $ CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */

#define RFIO_KERNEL     1
#include "rfio.h"
#include <Cpwd.h>

#include <errno.h>
#include <string.h>
#include <stdlib.h>            /* malloc prototype */

/*
 * returns -1 if an error occured or no information is available at the
 * daemon's host. 0 otherwise.
 */
int DLL_DECL rfio_unlink(n2 )
char *n2 ;

{
   int c;
   int status ;
   char *nbuf ;
   int s ;
   char *host ;
   char * filename;
   char *p ;
   int ans_req ,rt, parserc ;
   int rcode ;
   int uid ;
   int gid ;
   struct passwd *pw ;
   char buf[256];
   char *n1 = "";
   /*
    * The file is local.
    */
   INIT_TRACE("RFIO_TRACE");
   TRACE( 1, "rfio", " rfio_unlink (%s)",n2 );
   if ( ! (parserc = rfio_parseln(n2,&host,&filename,NORDLINKS)) ) {
       /* if not a remote file, must be local or HSM  */
       if ( host != NULL ) {
           /*
            * HSM file.
            */
           TRACE(1,"rfio","rfio_unlink: %s is an HSM path",
                 filename);
           END_TRACE();
           rfio_errno = 0;
           status = rfio_HsmIf_unlink(filename);
           return(status);
      }
      TRACE(2,"rfio","rfio_unlink local %s",filename);
      status = unlink(filename) ;
      if ( status < 0 ) serrno = 0;
      END_TRACE() ;
      rfio_errno = 0;
      return(status) ;
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
   if ( (pw = Cgetpwuid(uid) ) == NULL ) {
      TRACE(2, "rfio" ,"rfio_unlink: Cgetpwuid() error %s",
	    strerror(errno));
      END_TRACE();
      return -1 ;
   }

   p = buf ;
   marshall_WORD(p, B_RFIO_MAGIC);
   marshall_WORD(p, RQST_SYMLINK);

   status = strlen(pw->pw_name)+strlen(n1)+strlen(filename)+3+2*WORDSIZE;
   marshall_LONG(p, status) ;

   if (netwrite_timeout(s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "unlink: write(): ERROR occured (errno=%d)",
	    errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   nbuf = (char *) malloc( status ) ;
   if ( nbuf == NULL ) {
      TRACE(2, "rfio", "unlink:  malloc () failed");
      (void) close(s);
      END_TRACE();
      return(-1);
   }

   p = nbuf ;

   marshall_WORD(p,uid) ;
   marshall_WORD(p,gid) ;
   marshall_STRING( p, n1 ) ;
   marshall_STRING( p, filename ) ;
   marshall_STRING( p, pw->pw_name) ;
	
   if (netwrite_timeout(s,nbuf,status,RFIO_CTRL_TIMEOUT) != status ) {
      TRACE(2, "rfio", "unlink: write(): ERROR occured (errno=%d)",errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   (void) free(nbuf) ;

   /*
    * Getting back status
    */ 
   if ((c=netread_timeout(s, buf, WORDSIZE + 2*LONGSIZE, RFIO_CTRL_TIMEOUT)) != (WORDSIZE+ 2*LONGSIZE))  {
	 TRACE(2, "rfio", "rfio_unlink: read(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   p = buf;
   unmarshall_WORD( p, ans_req );
   unmarshall_LONG( p, status ) ;
   unmarshall_LONG( p, rcode ) ;

   if ( ans_req != RQST_SYMLINK ) {
      TRACE(1,"rfio","rfio_unlink: ERROR: answer does not correspond to request !");
      (void) close(s);
      END_TRACE();
      return(-1);
   }

   if ( status < 0 ) {
      TRACE(1,"rfio","rfio_unlink: failure, error %d",rcode);
      rfio_errno = rcode ;
      (void) close(s);
      END_TRACE();
      return(status);
   }
   TRACE (2,"rfio","rfio_unlink succeded");
   END_TRACE();
   (void) close (s) ;
   return(status) ;
}
