/*
 * $Id: symlink.c,v 1.11 2003/09/14 06:38:58 jdurand Exp $
 */


/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: symlink.c,v $ $Revision: 1.11 $ $Date: 2003/09/14 06:38:58 $ CERN/IT/PDP/DM Felix Hassine";
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
int DLL_DECL rfio_symlink(n1,n2 )
char *n1 ;
char *n2 ;

{
   int c;
   int status ;
   char *nbuf ;
   int s ;
   char *host ;
   char * filename;
   char *p ;
   int ans_req ,rt ;
   int rcode ;
   int uid ;
   int gid ;
   struct passwd *pw ;
   char buf[256];
   /*
    * The file is local.
    */
   INIT_TRACE("RFIO_TRACE");
   TRACE( 1, "rfio", " rfio_symlink (%s,%s)",n1,n2 );
   if ( ! rfio_parseln(n2,&host,&filename,NORDLINKS) ) {
       /* if not a remote file, must be local or HSM  */
       if ( host != NULL ) {
           /*
            * HSM file.
            */
           TRACE(1,"rfio","rfio_symlink: %s is an HSM path",
                 filename);
           END_TRACE();
           rfio_errno = 0;
           serrno = SEOPNOTSUP;
           status = -1;
           return(status);
      }
      TRACE(2,"rfio","rfio_symlink local %s -> %s",filename,n1);
#if ! defined(_WIN32)
      status = symlink(n1,filename) ;
      if ( status < 0 ) serrno = 0;
#else
      { serrno = SEOPNOTSUP; status = -1;}
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
   if ( (pw = Cgetpwuid(uid) ) == NULL ) {
      TRACE(2, "rfio" ,"rfio_symlink: Cgetpwuid() error %s",
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
      TRACE(2, "rfio", "symlink: write(): ERROR occured (errno=%d)",
	    errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   nbuf = (char *) malloc( status ) ;
   if ( nbuf == NULL ) {
      TRACE(2, "rfio", "symlink:  malloc () failed");
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
      TRACE(2, "rfio", "symlink: write(): ERROR occured (errno=%d)",errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   (void) free(nbuf) ;

   /*
    * Getting back status
    */ 
   if ((c=netread_timeout(s, buf, WORDSIZE + 2*LONGSIZE, RFIO_CTRL_TIMEOUT)) != (WORDSIZE+ 2*LONGSIZE))  {
      if (c == 0) {
	 serrno = SEOPNOTSUP;	/* symbolic links not supported on remote machine */
	 TRACE(2, "rfio", "rfio_symlink: read(): ERROR occured (serrno=%d)", serrno);
      } else
	 TRACE(2, "rfio", "rfio_symlink: read(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   p = buf;
   unmarshall_WORD( p, ans_req );
   unmarshall_LONG( p, status ) ;
   unmarshall_LONG( p, rcode ) ;

   if ( ans_req != RQST_SYMLINK ) {
      TRACE(1,"rfio","rfio_symlink: ERROR: answer does not correspond to request !");
      (void) close(s);
      END_TRACE();
      return(-1);
   }

   if ( status < 0 ) {
      TRACE(1,"rfio","rfio_symlink: failure, error %d",rcode);
      rfio_errno = rcode ;
      (void) close(s);
      END_TRACE();
      return(status);
   }
   TRACE (2,"rfio","rfio_symlink succeded");
   END_TRACE();
   (void) close (s) ;
   return(status) ;
}
