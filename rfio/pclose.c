/*
 * $Id: pclose.c,v 1.12 2003/09/14 06:38:57 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: pclose.c,v $ $Revision: 1.12 $ $Date: 2003/09/14 06:38:57 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* pclose.c      Remote command I/O - close a popened command 		*/

#if defined(_WIN32)
#define WEXITSTATUS(status) ((status>>8)&0xFF)
#else
#include <sys/wait.h>
#endif
#include <errno.h>
#include <string.h>
#define RFIO_KERNEL     1 
#include "rfio.h"        
#include "rfio_rfilefdt.h"

/*
 * remote pclose
 */
int DLL_DECL rfio_pclose(fs)    
RFILE 	*fs ;
{
   char   * p  ; 
   int  status, fss ;
   char     buf[256];       /* General input/output buffer          */

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_pclose(%x)", fs);

   /*
    * The file is local
    */
   if (rfio_rfilefdt_findptr(fs,FINDRFILE_WITH_SCAN) == -1 ) {
      TRACE(2, "rfio", "rfio_pclose: using local pclose") ; 
#if defined(_WIN32)
      status = _pclose(fs->fp_save);
#else		
      status= pclose(fs->fp_save);
#endif	
      if ( status < 0 ) serrno = 0;
      free((char *)fs);
      END_TRACE() ; 
      rfio_errno = 0;
      return status ;
   }

   /*
    * Checking magic number
    */
   if ( fs->magic != RFIO_MAGIC ) {
      serrno = SEBADVERSION ;
      free((char *)fs);
      END_TRACE();
      return -1;
   }

   /*
    * Sending request.
    */
   p= buf ;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_PCLOSE);
   TRACE(2, "rfio", "rfio_pclose: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(fs->s, buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      fss = fs->s;
      TRACE(2, "rfio", "rfio_pclose: write(): ERROR occured (errno=%d)", errno);
      (void) free((char *)fs) ;
      (void)close(fss) ;
      END_TRACE() ;
      return -1 ;
   }
   /*
    * Getting data from the network.
    */
   p = buf ;
   if ( netread_timeout( fs->s , buf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != 2*LONGSIZE)  {
      fss = fs->s;
      TRACE(2,"rfio", "pclose: write(): %s", strerror(errno));
      (void) free((char *)fs) ;
      (void)close(fss) ;
      END_TRACE() ;
      return -1 ;
   }
   unmarshall_LONG(p, status) ;
   unmarshall_LONG(p, rfio_errno) ;
   TRACE(3,"rfio", "rfio_pclose: status is %d, rfio_errno is %d",status,rfio_errno);
   /*
    * freeing RFILE pointer
    */
   fss = fs->s;
   (void) free((char *)fs) ;
   (void) close(fss) ;
   return status ;
}
