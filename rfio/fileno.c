/*
 * $Id: fileno.c,v 1.1 2002/12/02 06:35:44 baud Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fileno.c,v $ $Revision: 1.1 $ $Date: 2002/12/02 06:35:44 $ CERN/IT/DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/* fileno.c     Remote File I/O - map stream pointer to file descriptor */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1     
#include "rfio.h"    
#include "rfio_rfilefdt.h"


int DLL_DECL rfio_fileno(fp)
RFILE *fp;                      /* Remote file pointer                  */
{
   int     fd;


   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_fileno(%x)", fp);

   if ( fp == NULL ) {
      errno = EBADF;
      END_TRACE();
      return -1 ;
   }

   /*
    * Is the file local? this is the only way to detect it !
    */
   if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1)
      fd = fileno((FILE *)fp);	/* The file is local */
   else
      fd = fp->s;		/* The file is remote */
   END_TRACE() ; 
   return (fd) ; 
}
