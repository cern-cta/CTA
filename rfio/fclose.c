/*
 * $Id: fclose.c,v 1.7 2000/10/02 08:02:29 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fclose.c,v $ $Revision: 1.7 $ $Date: 2000/10/02 08:02:29 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
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
   int     status;


   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_fclose(%x)", fp);

   if ( fp == NULL ) {
      errno = EBADF;
      END_TRACE();
      return -1 ;
   }

   /*
    * The file is local : this is the only way to detect it !
    */
   if (rfio_rfilefdt_findentry(fp->s,FINDRFILE_WITH_SCAN) == -1) {
      status= fclose((FILE *)fp) ;
      END_TRACE() ; 
      rfio_errno = 0;
      return status ; 
   }

   /*
    * The file is remote
    */
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
