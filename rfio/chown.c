/*
 * $Id: chown.c,v 1.9 2004/01/23 10:27:45 jdurand Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: chown.c,v $ $Revision: 1.9 $ $Date: 2004/01/23 10:27:45 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* chown.c       Remote File I/O - Change a file owner */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

int DLL_DECL rfio_chown(file, owner, group)     /* Remote chown	                */
char		*file;          /* remote file path             */
int		owner ;		   /* Owner's uid */
int 		group ;		   /* Owner's gid */
{
	char     buf[256];       /* General input/output buffer          */
	register int    s;              /* socket descriptor            */
	int             status;         /* remote chown() status        */
	int     	len;
	char    	*host,
			*filename;
	char    	*p=buf;
	int 		rt ;
	int 		rcode, parserc ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_chown(%s, %d, %d)", file,owner,group);

	if (!(parserc = rfio_parseln(file,&host,&filename,NORDLINKS))) {
		if ( host != NULL ) {
			/*
			 * HSM file
			 */
			TRACE(1,"rfio","rfio_chown: %s is an HSM path",
				  filename);
			END_TRACE();
			rfio_errno = 0;
			return(rfio_HsmIf_chown(filename,owner,group));
		}
		TRACE(1, "rfio", "rfio_chown: using local chown(%s, %d, %d)",
			filename, owner, group);

		END_TRACE();
		rfio_errno = 0;
		status = chown(filename,owner, group);
		if ( status < 0 ) serrno = 0;
		return(status);
	}
	if (parserc < 0) {
		END_TRACE();
		return(-1);
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
