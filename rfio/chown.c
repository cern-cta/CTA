/*
 * Copyright (C) 1994 by CERN/CN/PDP/CS
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)chown.c	1.3 05/06/98 CERN CN-PDP Felix Hassine ";
#endif /* not lint */

/* chown.c       Remote File I/O - Change a file owner */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

static char     buf[256];       /* General input/output buffer          */

int  rfio_chown(file, owner, group)     /* Remote chown	                */
char		*file;          /* remote file path             */
int		owner ;		   /* Owner's uid */
int 		group ;		   /* Owner's gid */
{
	register int    s;              /* socket descriptor            */
	int             status;         /* remote chown() status        */
	int     	len;
	char    	*host,
			*filename;
	char    	*p=buf;
	int 		rt ;
	int 		rcode ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_chown(%s, %d, %d)", file,owner,group);

	if (!rfio_parseln(file,&host,&filename,NORDLINKS)) {
  /* if not a remote file, must be local  */
		TRACE(1, "rfio", "rfio_chown: using local chown(%s, %d, %d)",
			filename, owner, group);

		END_TRACE();
		rfio_errno = 0;
		return(chown(filename,owner, group));
	}

	s = rfio_connect(host,&rt);
	if (s < 0)      {
		END_TRACE();
		return(-1);
	}

	len = strlen(filename)+ 2* WORDSIZE + 1;
	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_CHOWN);
	marshall_WORD(p, geteuid());
	marshall_WORD(p, getegid());
	marshall_LONG(p, len);
	p= buf + RQSTSIZE;
	marshall_STRING(p, filename);
	marshall_WORD(p, owner) ;
	marshall_WORD(p, group);
	TRACE(2,"rfio","rfio_chown: sending %d bytes",RQSTSIZE+len) ;
	if (netwrite(s,buf,RQSTSIZE+len) != RQSTSIZE+len) {
		TRACE(2, "rfio", "rfio_chown: write(): ERROR occured (errno=%d)", errno);
		(void) close(s);
		END_TRACE();
		return(-1);
	}
	p = buf;
	TRACE(2, "rfio", "rfio_chown: reading %d bytes", LONGSIZE);
	if (netread(s, buf, 2* LONGSIZE) != 2 * LONGSIZE)  {
		TRACE(2, "rfio", "rfio_chown: read(): ERROR occured (errno=%d)", errno);
		(void) close(s);
		END_TRACE();
		return(-1);
	}
	unmarshall_LONG(p, status);
	unmarshall_LONG(p, rcode);
	TRACE(1, "rfio", "rfio_chown: return %d",status);
	rfio_errno = rcode;
	(void) close(s);
	if (status)     {
		END_TRACE();
		return(-1);
	}
	END_TRACE();
	return (0);
}
