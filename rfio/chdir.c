/*
 * $Id: chdir.c,v 1.3 2004/01/23 10:27:45 jdurand Exp $
 */

/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: chdir.c,v $ $Revision: 1.3 $ $Date: 2004/01/23 10:27:45 $ CERN/IT/PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/* chdir.c       Remote File I/O - change working directory             */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

int DLL_DECL rfio_chdir(dirpath)
char		*dirpath;       /* directory path               */
{
   char    	*filename;
   char    	*host;
   int    	rc;
   int      parserc;
   
   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_chdir(%s)", dirpath);

   if (!(parserc = rfio_parseln(dirpath, &host, &filename, NORDLINKS))) {
      /* if not a remote file, must be local or HSM  */
      if ( host != NULL ) {
          /*
           * HSM file
           */
          TRACE(1, "rfio", "rfio_chdir: %s is an HSM path", filename);
          END_TRACE();
          rfio_errno = 0;
          if ( (rc = rfio_HsmIf_chdir(filename)) == 0 )
              rfio_HsmIf_SetCwdServer(host);
          return(rc);
      }
      TRACE(1, "rfio", "rfio_chdir: using local chdir(%s)", filename);

      END_TRACE();
      rfio_errno = 0;
      if ( (rc = chdir(filename)) == 0 )
          rfio_HsmIf_SetCwdType(0);
      else
          serrno = 0;
      return(rc);
   }
   if (parserc < 0) {
	   END_TRACE();
	   return(-1);
   }

   END_TRACE();
   rfio_errno = 0;
   serrno = SEOPNOTSUP;
   return (-1);
}
