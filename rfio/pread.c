/*
 * $Id: pread.c,v 1.7 2000/10/02 08:02:31 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: pread.c,v $ $Revision: 1.7 $ $Date: 2000/10/02 08:02:31 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* pread.c      Remote command I/O - read from a popened command	*/

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include "rfio_rfilefdt.h"

int DLL_DECL rfio_pread(ptr, size, items, fp)    /* Remote file read    */
char    *ptr;                           /* buffer pointer               */
int     size, items;                    /* .. size items                */
RFILE   *fp;                            /* remote file pointer          */
{
	int   status ;
	int rcode;
	char *p ;
	char     buf[256];       /* General input/output buffer          */

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_pread(%x, %d, %d, %x)", ptr, size, items, fp);

        if (rfio_rfilefdt_findentry(fp->s,FINDRFILE_WITH_SCAN) == -1) {
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
