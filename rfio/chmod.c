/*
 * $Id: chmod.c,v 1.10 2004/03/03 11:15:57 obarring Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: chmod.c,v $ $Revision: 1.10 $ $Date: 2004/03/03 11:15:57 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/* chmod.c       Remote File I/O - change file mode                     */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

int DLL_DECL rfio_chmod(dirpath, mode)     /* Remote chmod	        */
char		*dirpath;          /* remote directory path             */
int		mode;              /* remote directory mode             */
{
	char     buf[BUFSIZ];       /* General input/output buffer          */
	register int    s;              /* socket descriptor            */
	int             status;         /* remote chmod() status        */
	int     	len;
	char    	*host,
			*filename;
	char    	*p=buf;
	int 		rt ;
	int 		rcode ;
	int         parserc;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_chmod(%s, %o)", dirpath, mode);

	if (!(parserc = rfio_parseln(dirpath,&host,&filename,NORDLINKS))) {
		/* if not a remote file, must be local or HSM  */
		if ( host != NULL ) {
			/*
			 * HSM file
			 */
			TRACE(1,"rfio","rfio_chmod: %s is an HSM path",
				  filename);
			END_TRACE();
			rfio_errno = 0;
			return(rfio_HsmIf_chmod(filename,mode));
		}
		TRACE(1, "rfio", "rfio_chmod: using local chmod(%s, %o)",
			  filename, mode);

		END_TRACE();
		rfio_errno = 0;
		status = chmod(filename,mode);
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

	len = strlen(filename)+ LONGSIZE + 1;
  if ( RQSTSIZE+len > BUFSIZ ) {
    TRACE(2,"rfio","rfio_chmod: request too long %d (max %d)",
          RQSTSIZE+len,BUFSIZ);
    END_TRACE();
    (void) netclose(s);
    serrno = E2BIG;
    return(-1);
  }
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
	if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
		TRACE(2, "rfio", "rfio_chmod: write(): ERROR occured (errno=%d)", errno);
		(void) netclose(s);
		END_TRACE();
		return(-1);
	}
	p = buf;
	TRACE(2, "rfio", "rfio_chmod: reading %d bytes", LONGSIZE);
	if (netread_timeout(s, buf, 2* LONGSIZE, RFIO_CTRL_TIMEOUT) != (2 * LONGSIZE))  {
		TRACE(2, "rfio", "rfio_chmod: read(): ERROR occured (errno=%d)", errno);
		(void) netclose(s);
		END_TRACE();
		return(-1);
	}
	unmarshall_LONG(p, status);
	unmarshall_LONG(p, rcode);
	TRACE(1, "rfio", "rfio_chmod: return %d",status);
	rfio_errno = rcode;
	(void) netclose(s);
	if (status)     {
		END_TRACE();
		return(-1);
	}
	END_TRACE();
	return (0);
}
