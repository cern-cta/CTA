/*
 * $Id: chown.c,v 1.3 1999/12/09 08:45:21 baran Exp $
 *
 * $Log: chown.c,v $
 * Revision 1.3  1999/12/09 08:45:21  baran
 * Thread-safe version
 *
 * Revision 1.2  1999/07/20 12:47:53  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1994 by CERN/CN/PDP/CS
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)chown.c	1.3 5/6/98 CERN CN-PDP Felix Hassine ";
#endif /* not lint */

/* chown.c       Remote File I/O - Change a file owner */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

int  rfio_chown(file, owner, group)     /* Remote chown	                */
char		*file;          /* remote file path             */
int		owner ;		   /* Owner's uid */
int 		group ;		   /* Owner's gid */
{
	static char     buf[256];       /* General input/output buffer          */
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
	if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
		TRACE(2, "rfio", "rfio_chown: write(): ERROR occured (errno=%d)", errno);
		(void) close(s);
		END_TRACE();
		return(-1);
	}
	p = buf;
	TRACE(2, "rfio", "rfio_chown: reading %d bytes", LONGSIZE);
	if (netread_timeout(s, buf, 2* LONGSIZE, RFIO_CTRL_TIMEOUT) != (2 * LONGSIZE))  {
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
