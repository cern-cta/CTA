/*
 * $Id: feof.c,v 1.4 2000/09/20 13:52:50 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: feof.c,v $ $Revision: 1.4 $ $Date: 2000/09/20 13:52:50 $ CERN/IT/PDP/DM Antoine Trannoy";
#endif /* not lint */

/* feof.c      Remote File I/O - tell if the eof has been reached       */

/*
 * System remote file I/O definitions
 */

#define RFIO_KERNEL     1  
#include "rfio.h"         

int rfio_feof(fp)
	RFILE * fp ; 
{
	int     rc 	;
	int i 		;
	int remoteio=0 	;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_feof(%x)", fp);

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

	/*
	 * The file is local
	 */
	if ( !remoteio ) {
		rc= feof((FILE *)fp) ; 
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
		END_TRACE() ;
		return -1 ;
	}
	
	/*
	 * The file is remote, using then the eof flag updated
 	 * by rfio_fread.
	 */
#ifdef linux
	if ( ((RFILE *)fp)->eof & _IO_EOF_SEEN ) 
#else
#ifdef __Lynx__
	if ( ((RFILE *)fp)->eof & _EOF ) 
#else
	if ( ((RFILE *)fp)->eof & _IOEOF ) 
#endif
#endif
		rc = 1 ;
	else 
		rc = 0 ;
	END_TRACE() ;
	return rc ; 
	
}

