/*
 * Copyright (C) 1990,1991 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)fclose.c	3.4 05/06/98  CERN CN-SW/DC F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fclose.c     Remote File I/O - close a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1     
#include "rfio.h"    

static char     buf[256];       /* General input/output buffer          */

int rfio_fclose(fp)             /* Remote file close                    */
RFILE *fp;                      /* Remote file pointer                  */
{
	char    *p = buf;
	int     status;
	int i, remoteio=0 ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_fclose(%x)", fp);

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
		status= fclose((FILE *)fp) ;
		END_TRACE() ; 
		rfio_errno = 0;
		return status ; 
	}

	/*
	 * The file is remote
	 */
	if ( fp->magic != RFIO_MAGIC ) {
		(void) close(fp->s);
		free((char *)fp);
		END_TRACE()  ; 
		return -1 ;
	}

	status= rfio_close(fp->s) ;
	END_TRACE() ;
	return status ;
}
