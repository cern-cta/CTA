/*
 * $Id: fflush.c,v 1.2 1999/07/20 12:47:57 jdurand Exp $
 *
 * $Log: fflush.c,v $
 * Revision 1.2  1999/07/20 12:47:57  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990,1991 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)fflush.c	3.4 04/06/99 CERN CN-SW/DC Antoine Trannoy";
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
