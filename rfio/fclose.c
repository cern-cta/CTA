/*
 * $Id: fclose.c,v 1.10 2001/07/04 11:37:40 baud Exp $
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fclose.c,v $ $Revision: 1.10 $ $Date: 2001/07/04 11:37:40 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fclose.c     Remote File I/O - close a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1     
#include "rfio.h"    
#include "rfio_rfilefdt.h"


int DLL_DECL rfio_fclose(fp)             /* Remote file close           */
RFILE *fp;                      /* Remote file pointer                  */
{
   static char     buf[256];       /* General input/output buffer          */
   char    *p = buf;
   int     HsmType;
   int     status;


   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_fclose(%x)", fp);

   if ( fp == NULL ) {
      errno = EBADF;
      END_TRACE();
      return -1 ;
   }

   /*
    * Check if file is Hsm. For CASTOR HSM files, the file is
    * closed using normal RFIO (local or remote) close().
    */
   /*
    * The file is local : this is the only way to detect it !
    */
   if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
      int fd = fileno((FILE *)fp);
      HsmType = rfio_HsmIf_GetHsmType(fd,NULL);
      if ( HsmType > 0 ) {
          status = rfio_HsmIf_close(fd);
          if ( HsmType != RFIO_HSM_CNS ) return(status);
      }
      status= fclose((FILE *)fp) ;
      END_TRACE() ; 
      rfio_errno = 0;
      return status ; 
   }

   /*
    * The file is remote
    */
   HsmType = rfio_HsmIf_GetHsmType(fp->s,NULL);
   if ( HsmType > 0 ) {
       status = rfio_HsmIf_close(fp->s);
       if ( HsmType != RFIO_HSM_CNS ) return(status);
   }
   if ( fp->magic != RFIO_MAGIC ) {
      int fps = fp->s;

      free((char *)fp);
      (void) close(fps);
      END_TRACE()  ; 
      return -1 ;
   }

   status= rfio_close(fp->s) ;
   END_TRACE() ;
   return status ;
}
