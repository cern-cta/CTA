/*
 * $Id: closedir.c,v 1.12 2002/09/20 06:59:34 baud Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: closedir.c,v $ $Revision: 1.12 $ $Date: 2002/09/20 06:59:34 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/* closedir.c      Remote File I/O - close a directory                     */

#define RFIO_KERNEL     1 
#include "rfio.h"        
#include "rfio_rdirfdt.h"

/*
 * remote directory close
 */

int DLL_DECL rfio_closedir(dirp)
RDIR *dirp;
{
   char     rfio_buf[BUFSIZ];
   WORD    req ; 
   LONG  rcode ;
   LONG msgsiz ;
   int s;
   int status;
   char *p;
   extern RDIR *rdirfdt[MAXRFD];
   int s_index;

   /*
    * Search internal table for this directory pointer
    */
   s_index = rfio_rdirfdt_findptr(dirp,FINDRDIR_WITH_SCAN);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_closedir(0x%x)", dirp);

   /*
    * The directory is local
    */
   if (s_index == -1) {
      TRACE(2, "rfio", "rfio_closedir: check if HSM directory");
      if ( rfio_HsmIf_IsHsmDirEntry((DIR *)dirp) != -1 ) {
          status = rfio_HsmIf_closedir((DIR *)dirp);
      } else {
          TRACE(2, "rfio", "rfio_closedir: using local closedir(0x%x)",dirp) ; 
#if defined(_WIN32)
          status = -1;
          serrno = SEOPNOTSUP;
#else /* _WIN32 */
          status= closedir((DIR *)dirp) ; 
          if ( status < 0 ) serrno = 0;
#endif /* _WIN32 */
      }
      END_TRACE() ; 
      return status ;
   }
   /*
    * Checking magic number
    */
   s = rdirfdt[s_index]->s;
   if ( rdirfdt[s_index]->magic != RFIO_MAGIC ) {
      serrno = SEBADVERSION ;
      rfio_rdirfdt_freeentry(s_index);
      (void) close(s) ;
      END_TRACE();
      return(-1);
   }
   /*
    * Sending request.
    */
   p= rfio_buf ;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_CLOSEDIR);
   TRACE(2, "rfio", "rfio_closedir: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s, rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_closedir: write(): ERROR occured (errno=%d)", errno);
      (void) rfio_dircleanup(s) ;
      END_TRACE() ;
      return -1 ;
   }
   /*
    * Getting data from the network.
    */

   TRACE(2, "rfio", "rfio_closedir: reading %d bytes",WORDSIZE+3*LONGSIZE) ; 
   if (netread_timeout(s,rfio_buf,WORDSIZE+3*LONGSIZE,RFIO_CTRL_TIMEOUT) != (WORDSIZE+3*LONGSIZE)) {
      TRACE(2, "rfio", "rfio_closedir: read(): ERROR occured (errno=%d)", errno);
      (void)rfio_dircleanup(s) ; 
      END_TRACE() ;
      return -1 ; 
   }
   p = rfio_buf ;
   unmarshall_WORD(p,req) ;
   unmarshall_LONG(p,status) ;
   unmarshall_LONG(p, rcode) ;
   unmarshall_LONG(p,msgsiz) ;
   rfio_errno = rcode ;
   (void) rfio_dircleanup(s) ; 
   TRACE(1, "rfio", "rfio_closedir: return status=%d, rcode=%d",status,rcode) ;
   END_TRACE() ; 
   return status ;
}
