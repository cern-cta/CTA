/*
 * $Id: ferror.c,v 1.4 2001/06/20 09:40:02 baud Exp $
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: ferror.c,v $ $Revision: 1.4 $ $Date: 2001/06/20 09:40:02 $ CERN/IT/PDP/DM Antoine Trannoy";
#endif /* not lint */

/* ferror.c      Remote File I/O - tell if an error happened            */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include "rfio_rfilefdt.h"

int rfio_ferror(fp)
	RFILE * fp ; 
{
	int     rc 	;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_ferror(%x)", fp);

	if ( fp == NULL ) {
		errno = EBADF;
		END_TRACE();
		return -1 ;
	}

	/*
	 * The file is local
	 */
	if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1 ) {
		rc= ferror((FILE *)fp) ; 
		END_TRACE() ; 
		rfio_errno = 0;
		return rc ;
	}

	/*
	 * Checking magic number
	 */
	if ( fp->magic != RFIO_MAGIC) {
		int fps = fp->s;
		serrno = SEBADVERSION ; 
		free((char *)fp);
		(void) close(fps) ;
		END_TRACE() ;
		return -1 ;
	}
	
	/*
	 * The file is remote, using then the eof flag updated
 	 * by rfio_fread.
	 */
#ifdef linux
	if ( ((RFILE *)fp)->eof & _IO_ERR_SEEN ) 
#else
#ifdef __Lynx__
	if ( ((RFILE *)fp)->eof & _ERR ) 
#else
	if ( ((RFILE *)fp)->eof & _IOERR ) 
#endif
#endif
		rc = 1 ;
	else 
		rc = 0 ;
	END_TRACE() ;
	return rc ; 
}

