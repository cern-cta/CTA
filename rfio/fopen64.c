/*
 * $Id: fopen64.c,v 1.1 2002/11/19 10:51:22 baud Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fopen64.c,v $ $Revision: 1.1 $ $Date: 2002/11/19 10:51:22 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fopen64.c      Remote File I/O - open a binary file                    */

/*
 * System remote file I/O
 */
#define RFIO_KERNEL     1
#include <fcntl.h>
#include "rfio.h"    
#include "rfio_rfilefdt.h"


/*
 * Remote file open
 */
RFILE DLL_DECL *rfio_fopen64(file, mode) 
	char * file ;  
	char * mode ; 
{
	register int f, rw, oflags ;
	int f_index;

	INIT_TRACE("RFIO_TRACE") ;
	TRACE(1, "rfio", "rfio_fopen64(%s, %s)", file, mode) ;

	if (mode[1] == 'b')
		rw= ( mode[2] == '+' ) ; 
	else
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
	
	f= rfio_open64(file,oflags, 0666) ; 
	if ( f < 0 ) {
		END_TRACE() ;
		return NULL ;
	}

	if ( (f_index = rfio_rfilefdt_findentry(f,FINDRFILE_WITHOUT_SCAN)) != -1  ) {
		END_TRACE() ; 
		return (RFILE *) rfilefdt[f_index] ;
	}
	else {
		TRACE(3,"rfio","rfio_fopen64() : Using local FILE ptr ");
		END_TRACE() ; 
		rfio_errno = 0;
		return (RFILE *) fdopen(f,mode);
	}
}
