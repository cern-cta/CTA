/*
 * $Id: pclose.c,v 1.8 2000/10/02 08:02:31 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: pclose.c,v $ $Revision: 1.8 $ $Date: 2000/10/02 08:02:31 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* pclose.c      Remote command I/O - close a popened command 		*/

#if defined(_WIN32)
#define WEXITSTATUS(status) ((status>>8)&0xFF)
#else
#include <sys/wait.h>
#endif
#define RFIO_KERNEL     1 
#include "rfio.h"        
#include "rfio_rfilefdt.h"
#ifndef linux
extern char     *sys_errlist[]; /* External error list          	*/
#endif

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
   if (rfio_rfilefdt_findentry(fs->s,FINDRFILE_WITH_SCAN) == -1 ) {
      TRACE(2, "rfio", "rfio_pclose: using local pclose") ; 
#if defined(_WIN32)
      status = _pclose(fs->fp_save);
#else		
      status= pclose(fs->fp_save);
#endif	
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
      TRACE(2,"rfio", "pclose: write(): %s", sys_errlist[errno]);
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
   (void) close(fs->s) ;
   return status ;
}
