/*
 * $Id: fread.c,v 1.9 2002/09/20 06:59:34 baud Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fread.c,v $ $Revision: 1.9 $ $Date: 2002/09/20 06:59:34 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fread.c      Remote File I/O - write a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1 
#include "rfio.h"    
#include "rfio_rfilefdt.h"

/*
 * Remote file read
 */
int DLL_DECL rfio_fread(ptr, size, items, fp)  
	void    *ptr;                           /* buffer pointer               */
	int     size, items;                    /* .. size items                */
	RFILE   *fp;                            /* remote file pointer          */
{
	int	rc ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_fread(%x, %d, %d, %x)", ptr, size, items, fp);

	/*
	 * Checking fp validity
	 */
	if ( fp == NULL ) {
		errno = EBADF ;
		TRACE(2,"rfio","rfio_fread() : FILE ptr is NULL ") ;
		END_TRACE() ;
		return 0 ;
	}

	if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
		TRACE(2,"rfio","rfio_fread() : using local fread() ") ;
		rfio_errno = 0;
		rc= fread(ptr, size, items, (FILE *)fp) ;
		if ( rc == 0 ) serrno = 0;
		END_TRACE() ; 
		return rc ;
	}

	TRACE(2,"rfio","rfio_fread() : ------------>2") ;

	/*
	 * Checking magic number
	 */
	if ( fp->magic != RFIO_MAGIC) {
		int fps = fp->s;
		serrno = SEBADVERSION ; 
		TRACE(2,"rfio","rfio_fread() : Bad magic number  ") ;
		free((char *)fp);
		(void) close(fps) ;
		END_TRACE();
		return 0 ;
	}

	/*
	 * The file is remote 
	 */
	rc= rfio_read(fp->s,ptr,size*items) ;
	switch(rc) {
		case -1:
#ifdef linux
			((RFILE *)fp)->eof |= _IO_ERR_SEEN ;
#else
#ifdef __Lynx__
			((RFILE *)fp)->eof |= _ERR ;
#else
			((RFILE *)fp)->eof |= _IOERR ;
#endif
#endif
			rc= 0 ; 
			break ; 
		case 0:
#ifdef linux
			((RFILE *)fp)->eof |= _IO_EOF_SEEN ; 
#else
#ifdef __Lynx__
			((RFILE *)fp)->eof |= _EOF ; 
#else
			((RFILE *)fp)->eof |= _IOEOF ; 
#endif
#endif
			break ; 
		default:
			rc= (rc+size-1)/size ;
			break ; 
	}
	END_TRACE() ;
	return rc ; 
}
