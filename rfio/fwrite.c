/*
 * Copyright (C) 1990-1998 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)fwrite.c	3.8 04/06/99 CERN CN-SW/DC F. Hemmer, A. Trannoy";
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
int rfio_fwrite(ptr, size, items, fp) 
	char    *ptr;          /* buffer pointer */
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
		serrno = SEBADVERSION ; 
		(void) close(fp->s) ;
		free((char *)fp);
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
