/*
 * $Id: fileno.c,v 1.3 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/* fileno.c     Remote File I/O - map stream pointer to file descriptor */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"


int rfio_fileno(RFILE *fp)                      /* Remote file pointer                  */
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
    fd = fileno((FILE *)fp); /* The file is local */
  else
    fd = fp->s;  /* The file is remote */
  END_TRACE() ;
  return (fd) ;
}
