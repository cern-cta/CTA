/*
 * $Id: rmdir.c,v 1.5 2000/05/03 13:42:37 obarring Exp $
 */

/*
 * Copyright (C) 1998-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rmdir.c,v $ $Revision: 1.5 $ $Date: 2000/05/03 13:42:37 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/* rmdir.c       Remote File I/O - make a directory file                */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */


int  rfio_rmdir(dirpath)           /* Remote rmdir	                */
char		*dirpath;          /* remote directory path             */
{
	static char     buf[256];       /* General input/output buffer          */
	register int    s;              /* socket descriptor            */
	int             status;         /* remote rmdir() status        */
	int     	len;
	char    	*host,
			*filename;
	char    	*p=buf;
	int 		rt ;
	int 		rcode ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_rmdir(%s)", dirpath);

	if (!rfio_parseln(dirpath,&host,&filename,NORDLINKS)) {
                /* if not a remote file, must be local or HSM  */
                if ( host != NULL ) {
                    /*
                     * HSM file
                     */
                    TRACE(1,"rfio","rfio_rmdir: %s is an HSM path",
                          dirpath);
                    END_TRACE();
                    rfio_errno = 0;
                    return(rfio_HsmIf_rmdir(dirpath));
                }

		TRACE(1, "rfio", "rfio_rmdir: using local rmdir(%s)",
			filename);

		END_TRACE();
		rfio_errno = 0;
		return(rmdir(filename));
	}

	s = rfio_connect(host,&rt);
	if (s < 0)      {
		END_TRACE();
		return(-1);
	}

	len = strlen(filename) + 1;
	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_RMDIR);
	marshall_WORD(p, geteuid());
	marshall_WORD(p, getegid());
	marshall_LONG(p, len);
	p= buf + RQSTSIZE;
	marshall_STRING(p, filename);
	TRACE(2,"rfio","rfio_rmdir: sending %d bytes",RQSTSIZE+len) ;
	if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
		TRACE(2, "rfio", "rfio_rmdir: write(): ERROR occured (errno=%d)", errno);
		(void) close(s);
		END_TRACE();
		return(-1);
	}
	p = buf;
	TRACE(2, "rfio", "rfio_rmdir: reading %d bytes", LONGSIZE);
	if (netread_timeout(s, buf, 2* LONGSIZE, RFIO_CTRL_TIMEOUT) != (2 * LONGSIZE))  {
		TRACE(2, "rfio", "rfio_rmdir: read(): ERROR occured (errno=%d)", errno);
		(void) close(s);
		END_TRACE();
		return(-1);
	}
	unmarshall_LONG(p, status);
	unmarshall_LONG(p, rcode);
	TRACE(1, "rfio", "rfio_rmdir: return %d",status);
	rfio_errno = rcode;
	(void) close(s);
	if (status)     {
		END_TRACE();
		return(-1);
	}
	END_TRACE();
	return (0);
}
