/*
 * $Id: pread.c,v 1.2 1999/07/20 12:48:05 jdurand Exp $
 *
 * $Log: pread.c,v $
 * Revision 1.2  1999/07/20 12:48:05  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1993-1998 by CERN CN-SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)pread.c	1.7 09/03/98  CERN CN-SW/DC Felix Hassine";
#endif /* not lint */

/* pread.c      Remote command I/O - read from a popened command	*/

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

static char     buf[256];       /* General input/output buffer          */

int rfio_pread(ptr, size, items, fp)    /* Remote file read             */
char    *ptr;                           /* buffer pointer               */
int     size, items;                    /* .. size items                */
RFILE   *fp;                            /* remote file pointer          */
{
	int   status ;
	int rcode,i,remoteio=0 ;
	char *p ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_pread(%x, %d, %d, %x)", ptr, size, items, fp);

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
		TRACE(3,"rfio","local pread(%x,%d,%d,%x)",ptr, size, items, &(fp->fp));
		status = fread(ptr, size, items, fp->fp_save) ;
		END_TRACE();
		rfio_errno = 0;
		if ( status > 0) ptr[status]= '\0' ;
		return status ;
	}

	p = buf ;
	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_FREAD);
	marshall_LONG(p, size);
	marshall_LONG(p, items);
	TRACE(3, "rfio", "rfio_pread: sending %d bytes", 2*WORDSIZE+2*LONGSIZE);
	if (netwrite_timeout(fp->s, buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE )     {
		TRACE(2, "rfio", "rfio_pread: write(): ERROR occured (errno=%d)", errno);
		END_TRACE();
		return -1 ;
	}
	p = buf;
	TRACE(3, "rfio", "rfio_pread: reading %d bytes", 2*LONGSIZE);
	if (netread_timeout(fp->s, buf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != (2*LONGSIZE))  {
		TRACE(2, "rfio", "rfio_pread: read(): ERROR occured (errno=%d)", errno);
		END_TRACE();
		return -1 ;
	}
	unmarshall_LONG(p, status);
	unmarshall_LONG(p, rcode);
	rfio_errno = rcode ;
	TRACE(1, "rfio", "rfio_pread: status %d, rfio_errno %d", status, rfio_errno);
	if ( status > 0 ) {
		TRACE(2, "rfio", "rfio_pread: reading %d bytes", status*size);
		if (netread_timeout(fp->s, ptr, status*size, RFIO_DATA_TIMEOUT) != (status*size))       {
			TRACE(2, "rfio", "rfio_pread: read(): ERROR occured (errno=%d)", errno);
			END_TRACE();
			return -1 ;
		}
	}
	END_TRACE();
	return(status);
}
