/*
 * $Id: rename.c,v 1.13 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* rename.c       Remote File I/O - change the name of a file           */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */
#include <sys/param.h>
#include "rfio.h"               /* Remote File I/O general definitions  */
#include <string.h>
#include <unistd.h>

/*
** NB This does not implement a rename across hosts
*/

/* Remote rename               */
int  rfio_rename(char  *fileo,  /* remote old path     */
                 char *filen)  /* remote new path              */
{
  int             status;         /* remote rename() status       */
  char     hostnameo[MAXHOSTNAMELEN],
    hostnamen[MAXHOSTNAMELEN],
    filenameo[MAXFILENAMSIZE],
    filenamen[MAXFILENAMSIZE];
  char  *host, *path;
  int parserc ;
  int rpo, rpn;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_rename(%s, %s)", fileo, filen);

  *hostnameo = *hostnamen = '\0';
  rpo = rfio_parseln(fileo,&host,&path,NORDLINKS);

  if (host != NULL)
    strcpy(hostnameo, host);

  strcpy(filenameo, path);

  rpn = parserc = rfio_parse(filen,&host, &path);

  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  if (host != NULL)
    strcpy(hostnamen, host);

  strcpy(filenamen, path);

  /*
  ** We do not allow a rename across hosts as this implies
  ** a copy (cf move(1)). This may change in the future.
  */
  if ((!rpo && rpn) || (rpo && !rpn)) {
    serrno = SEXHOST;
    END_TRACE();
    return(-1);
  }

  if (rpo && rpn)
  { if (strcmp(hostnameo, hostnamen) != 0) {
      serrno = SEXHOST;
      END_TRACE();
      return(-1);
    }
  }

  if ((!rpo) && (!rpn)) {
    /* if not a remote file, must be local or HSM  */
    if ( *hostnameo != '\0' && *hostnamen != '\0' ) {
      /*
       * HSM file
       */
      TRACE(1,"rfio","rfio_rename: %s and %s are HSM paths",
            filenameo,filenamen);
      END_TRACE();
      rfio_errno = 0;
      return(rfio_HsmIf_rename(filenameo,filenamen));
    }
    /* if not remote files, must be local  */
    TRACE(1, "rfio", "rfio_rename: using local rename(%s, %s)",
          filenameo, filenamen);

    END_TRACE();
    rfio_errno = 0;
    status = rename(filenameo,filenamen);
    if ( status < 0 ) serrno = 0;
    return(status);
  }

  // Remote call. Not supported anymore
  TRACE(1, "rfio", "rfio_rename: return %d",SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return(-1);
}
