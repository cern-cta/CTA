/*
 * $Id: fflush.c,v 1.3 1999/12/09 13:46:43 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fflush.c,v $ $Revision: 1.3 $ $Date: 1999/12/09 13:46:43 $ CERN/IT/PDP/DM Antoine Trannoy";
#endif /* not lint */

/* fflush.c     Remote File I/O - flush a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1   
#include "rfio.h"          

/*
 * Remote file flush
 * If the file is remote, this is a dummy operation.
 */
int rfio_fflush(fp)      
	RFILE *fp;             
{
	int     status;
	int	i, remoteio = 0 ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_fflush(%x)", fp);

	if ( fp == NULL ) {
		errno = EBADF;
		END_TRACE();
		return -1 ;
	}

	/*
	 * The file is local : this is the only way to detect it !
	 */
	for ( i=0 ; i< MAXRFD  ; i++ ) {
		if ( rfilefdt[i] == fp ) {
			remoteio ++ ;
			break ;
		}
	}
	if ( !remoteio ) {
		status= fflush((FILE *)fp) ;
		END_TRACE() ; 
		rfio_errno = 0;
		return status ;
	}

	/*
	 * Checking magic number
	 */
	if ( fp->magic != RFIO_MAGIC) {
		serrno = SEBADVERSION ; 
		(void) close(fp->s) ;
		free((char *)fp);
		END_TRACE() ;
		return -1 ;
	}
	END_TRACE();
	return 0 ;
}
