/*
 * Copyright (C) 1998 by CERN/IT/PDP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)chmod.c	1.2 01/21/99  CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/* chmod.c       Remote File I/O - change file mode                     */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

static char     buf[256];       /* General input/output buffer          */

int  rfio_chmod(dirpath, mode)     /* Remote chmod	                */
char		*dirpath;          /* remote directory path             */
int		mode;              /* remote directory mode             */
{
	register int    s;              /* socket descriptor            */
	int             status;         /* remote chmod() status        */
	int     	len;
	char    	*host,
			*filename;
	char    	*p=buf;
	int 		rt ;
	int 		rcode ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_chmod(%s, %o)", dirpath, mode);

	if (!rfio_parseln(dirpath,&host,&filename,NORDLINKS)) {
  /* if not a remote file, must be local  */
		TRACE(1, "rfio", "rfio_chmod: using local chmod(%s, %o)",
			filename, mode);

		END_TRACE();
		rfio_errno = 0;
		return(chmod(filename,mode));
	}

	s = rfio_connect(host,&rt);
	if (s < 0)      {
		END_TRACE();
		return(-1);
	}

	len = strlen(filename)+ LONGSIZE + 1;
	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_CHMOD);
	marshall_WORD(p, geteuid());
	marshall_WORD(p, getegid());
	marshall_LONG(p, len);
	p= buf + RQSTSIZE;
	marshall_STRING(p, filename);
	marshall_LONG(p, mode);
	TRACE(1,"rfio","rfio_chmod: mode %o",mode);
	TRACE(2,"rfio","rfio_chmod: sending %d bytes",RQSTSIZE+len) ;
	if (netwrite(s,buf,RQSTSIZE+len) != RQSTSIZE+len) {
		TRACE(2, "rfio", "rfio_chmod: write(): ERROR occured (errno=%d)", errno);
		(void) close(s);
		END_TRACE();
		return(-1);
	}
	p = buf;
	TRACE(2, "rfio", "rfio_chmod: reading %d bytes", LONGSIZE);
	if (netread(s, buf, 2* LONGSIZE) != 2 * LONGSIZE)  {
		TRACE(2, "rfio", "rfio_chmod: read(): ERROR occured (errno=%d)", errno);
		(void) close(s);
		END_TRACE();
		return(-1);
	}
	unmarshall_LONG(p, status);
	unmarshall_LONG(p, rcode);
	TRACE(1, "rfio", "rfio_chmod: return %d",status);
	rfio_errno = rcode;
	(void) close(s);
	if (status)     {
		END_TRACE();
		return(-1);
	}
	END_TRACE();
	return (0);
}
