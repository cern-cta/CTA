/*
 * $Id: fwrite.c,v 1.6 2000/09/20 13:52:51 jdurand Exp $
 */

/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fwrite.c,v $ $Revision: 1.6 $ $Date: 2000/09/20 13:52:51 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fwrite.c     Remote File I/O - write a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1    
#include "rfio.h" 

/*
 * Remote file buffered write
 */
int DLL_DECL rfio_fwrite(ptr, size, items, fp) 
	void    *ptr;          /* buffer pointer */
	int     size, items; 
	RFILE   *fp;    
{
	int rc ;
	int i ;
	int remoteio = 0 ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_fwrite(%x, %d, %d, %x)", ptr, size, items, fp);

	if (fp == NULL ) { 
		errno = EBADF ;
		END_TRACE() ;
		return 0 ; 
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
		rc= fwrite(ptr, size, items, (FILE *)fp) ;
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
		END_TRACE();
		return 0 ;
	}

	/*
	 * The file is remote 
	 */
	rc= rfio_write(fp->s,ptr,size*items) ;
	switch(rc) {
		case -1:
#ifdef linux
			((FILE *)fp)->_flags |= _IO_ERR_SEEN ;
#else
#ifdef __Lynx__
			((FILE *)fp)->_flag |= _ERR ;
#else
			((FILE *)fp)->_flag |= _IOERR ;
#endif
#endif
			rc= 0 ; 
			break ; 
		case 0:
#ifdef linux
			((FILE *)fp)->_flags |= _IO_EOF_SEEN ; 
#else
#ifdef __Lynx__
			((FILE *)fp)->_flag |= _EOF ; 
#else
			((FILE *)fp)->_flag |= _IOEOF ; 
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
