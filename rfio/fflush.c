/*
 * $Id: fflush.c,v 1.7 2000/11/20 15:01:17 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fflush.c,v $ $Revision: 1.7 $ $Date: 2000/11/20 15:01:17 $ CERN/IT/PDP/DM Antoine Trannoy";
#endif /* not lint */

/* fflush.c     Remote File I/O - flush a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1   
#include "rfio.h"          
#include "rfio_rfilefdt.h"

/*
 * Remote file flush
 * If the file is remote, this is a dummy operation.
 */
int DLL_DECL rfio_fflush(fp)      
	RFILE *fp;             
{
	int     status;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_fflush(%x)", fp);

	if ( fp == NULL ) {
		errno = EBADF;
		END_TRACE();
		return -1 ;
	}

	if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1 ) {
		status= fflush((FILE *)fp) ;
		END_TRACE() ; 
		rfio_errno = 0;
		return status ;
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
	END_TRACE();
	return 0 ;
}
