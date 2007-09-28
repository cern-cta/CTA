/*
 * $Id: getcwd.c,v 1.4 2007/09/28 15:04:32 sponcec3 Exp $
 */

/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#if defined(_WIN32)
#include <direct.h>
#endif
/* getcwd.c      Remote File I/O - get current working directory        */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

char DLL_DECL *rfio_getcwd(char *buf, int size)
{
   char *p;
   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_getcwd()");

   if ( rfio_HsmIf_GetCwdType() > 0 ) {
          /*
           * HSM file
           */
          TRACE(1, "rfio", "rfio_getcwd: current working directory is an HSM path");
          END_TRACE();
          rfio_errno = 0;
          return(rfio_HsmIf_getcwd(buf, size));
   }
   TRACE(1, "rfio", "rfio_getcwd: using local getcwd()");

   END_TRACE();
   rfio_errno = 0;
   p = getcwd(buf, size);
   if ( ! p ) serrno = 0;
   return(p);
}
