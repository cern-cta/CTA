/*
 * $Id: fopen.c,v 1.7 2007/09/28 15:04:32 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* fopen.c      Remote File I/O - open a binary file                    */

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
RFILE DLL_DECL *rfio_fopen(file, mode) 
	char * file ;  
	char * mode ; 
{
	register int f, rw, oflags ;
    int f_index;

	INIT_TRACE("RFIO_TRACE") ;
	TRACE(1, "rfio", "rfio_fopen(%s, %s)", file, mode) ;

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
	
	f= rfio_open(file,oflags, 0666) ; 
	if ( f < 0 ) {
		END_TRACE() ;
		return NULL ;
	}

	if ( (f_index = rfio_rfilefdt_findentry(f,FINDRFILE_WITHOUT_SCAN)) != -1  ) {
		END_TRACE() ; 
		return (RFILE *) rfilefdt[f_index] ;
	}
	else {
		TRACE(3,"rfio","rfio_fopen() : Using local FILE ptr ");
		END_TRACE() ; 
		rfio_errno = 0;
		return (RFILE *) fdopen(f,mode);
	}
}
