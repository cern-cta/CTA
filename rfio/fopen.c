/*
 * $Id: fopen.c,v 1.2 1999/07/20 12:47:58 jdurand Exp $
 *
 * $Log: fopen.c,v $
 * Revision 1.2  1999/07/20 12:47:58  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990-1997 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)fopen.c	3.6 5/6/98 CERN CN-SW/DC F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fopen.c      Remote File I/O - open a binary file                    */

/*
 * System remote file I/O
 */
#define RFIO_KERNEL     1
#include <fcntl.h>
#include "rfio.h"    


/*
 * Remote file open
 */
RFILE   *rfio_fopen(file, mode) 
	char * file ;  
	char * mode ; 
{
	register int f, rw, oflags ;

	INIT_TRACE("RFIO_TRACE") ;
	TRACE(1, "rfio", "rfio_fopen(%s, %s)", file, mode) ;

	rw= ( mode[1] == '+' ) ; 
	switch(*mode) {
		case 'a':
			oflags= O_APPEND | O_CREAT | ( rw ? O_RDWR : O_WRONLY ) ;
			break ; 
		case 'r':
			oflags= rw ? O_RDWR : O_RDONLY ;
			break ; 
		case 'w':
			oflags= O_TRUNC | O_CREAT | ( rw ? O_RDWR : O_WRONLY ) ; 
			break ; 
		default:
			END_TRACE() ;
			return NULL ;
	}
	
	f= rfio_open(file,oflags, 0666) ; 
	if ( f < 0 ) {
		END_TRACE() ;
		return NULL ;
	}

	if ( rfilefdt[f] != NULL  ) {
		END_TRACE() ; 
		return (RFILE *) rfilefdt[f] ;
	}
	else {
		TRACE(3,"rfio","rfio_fopen() : Using local FILE ptr ");
		END_TRACE() ; 
		rfio_errno = 0;
		return (RFILE *) fdopen(f,mode);
	}
}
