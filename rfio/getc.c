/*
 * $Id: getc.c,v 1.1 2001/06/20 09:41:19 baud Exp $
 */

/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getc.c,v $ $Revision: 1.1 $ $Date: 2001/06/20 09:41:19 $ CERN/IT/PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/* get.c        Remote File I/O - input of one character                */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1 
#include "rfio.h"    
#include "rfio_rfilefdt.h"

/*
 * Remote file read
 */
int DLL_DECL rfio_getc(fp)  
	RFILE   *fp;                    /* remote file pointer          */
{
	unsigned char	c ;
	int	rc ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_getc(%x)", fp);

	/*
	 * Checking fp validity
	 */
	if ( fp == NULL ) {
		errno = EBADF ;
		TRACE(2,"rfio","rfio_getc() : FILE ptr is NULL ") ;
		END_TRACE() ;
		return 0 ;
	}

	if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
		TRACE(2,"rfio","rfio_getc() : using local getc() ") ;
		rfio_errno = 0;
		rc= getc((FILE *)fp) ;
		END_TRACE() ; 
		return rc ;
	}

	TRACE(2,"rfio","rfio_getc() : ------------>2") ;

	/*
	 * Checking magic number
	 */
	if ( fp->magic != RFIO_MAGIC) {
		int fps = fp->s;
		serrno = SEBADVERSION ; 
		TRACE(2,"rfio","rfio_getc() : Bad magic number  ") ;
		free((char *)fp);
		(void) close(fps) ;
		END_TRACE();
		return 0 ;
	}

	/*
	 * The file is remote 
	 */
	rc= rfio_read(fp->s,&c,1) ;
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
			rc= EOF ; 
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
			rc= EOF ; 
			break ; 
		default:
			rc= (int) c ;
			break ; 
	}
	END_TRACE() ;
	return rc ; 
}
