/*
 * $Id: fclose.c,v 1.14 2004/03/03 11:15:58 obarring Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fclose.c,v $ $Revision: 1.14 $ $Date: 2004/03/03 11:15:58 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fclose.c     Remote File I/O - close a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1     
#include "Castor_limits.h"    
#include "rfio.h"    
#include "rfio_rfilefdt.h"


int DLL_DECL rfio_fclose(fp)             /* Remote file close           */
RFILE *fp;                      /* Remote file pointer                  */
{
   int     HsmType;
   int     save_errno;
   int     status;
   int     status1;


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
      char upath[CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2];
      int fd = fileno((FILE *)fp);
      HsmType = rfio_HsmIf_GetHsmType(fd,NULL);
      if ( HsmType > 0 ) {
          if ( HsmType != RFIO_HSM_CNS ) {
              status = rfio_HsmIf_close(fd);
              END_TRACE() ; 
              return(status);
          }
          status1 = rfio_HsmIf_getipath(fd,upath);
      }
      status= fclose((FILE *)fp) ;
      if ( status < 0 ) serrno = 0;
      save_errno = errno;
      if ( HsmType == RFIO_HSM_CNS ) {
          if ( status1 == 1 ) {
              status1 = rfio_HsmIf_reqtoput(upath);
              if ( status1 == 0 ) errno = save_errno;
          }
      } else {
          status1 = 0;
      }
      END_TRACE() ; 
      rfio_errno = 0;
      return (status ? status : status1) ; 
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
