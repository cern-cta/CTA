/*
 * $Id: access.c,v 1.8 2002/09/20 06:59:33 baud Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: access.c,v $ $Revision: 1.8 $ $Date: 2002/09/20 06:59:33 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* access.c       Remote File I/O - get access status 			*/
#define RFIO_KERNEL	1
#include "rfio.h"               /* Remote File I/O general definitions  */


int DLL_DECL rfio_access(filepath, mode)       /* Remote file access            */
char    *filepath;              /* remote file path                     */
int 	mode ;			/* Access mode 				*/
{
	char     buf[256];       /* General input/output buffer          */
	register int    s;              /* socket descriptor            */
	int             status;         /* remote fopen() status        */
	int     	len;
	char    	*host, *filename;
	char    	*p=buf;
	int 		rt;
	int		uid ;
	int		gid ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_access(%s, %d)", filepath, mode);

	if (!rfio_parseln(filepath,&host,&filename,NORDLINKS)) {
                /* if not a remote file, must be local or HSM  */
                if ( host != NULL ) {
                    /*
                     * HSM file
                     */
                    TRACE(1,"rfio","rfio_access: %s is an HSM path",filename);
                    END_TRACE();
                    rfio_errno = 0;
                    return(rfio_HsmIf_access(filename,mode));
                }
		TRACE(1, "rfio", "rfio_access: using local access(%s, %d)",
			filename, mode);
		END_TRACE();
		rfio_errno = 0;
		status = access(filename,mode);
		if ( status < 0 ) serrno = 0;
		return(status);
	}

	s = rfio_connect(host,&rt);
	if (s < 0)      {
		END_TRACE();
		return(-1);
	}

	len = strlen(filename)+ 3*LONGSIZE+1;
	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_ACCESS);
	marshall_LONG(p, len);
	uid = geteuid() ;
	gid = getegid() ;
	p= buf + RQSTSIZE;
	marshall_STRING(p, filename);
	marshall_LONG(p, uid) ;
	marshall_LONG(p, gid) ;
	marshall_LONG(p, mode);
	TRACE(2,"rfio","rfio_access: sending %d bytes",RQSTSIZE+len) ;
	if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
		TRACE(2, "rfio", "rfio_access: write(): ERROR occured (errno=%d)", errno);
		(void) netclose(s);
		END_TRACE();
		return(-1);
	}
	p = buf;
	TRACE(2, "rfio", "rfio_access: reading %d bytes", LONGSIZE);
	if (netread_timeout(s, buf, LONGSIZE, RFIO_CTRL_TIMEOUT) != LONGSIZE )  {
		TRACE(2, "rfio", "rfio_access: read(): ERROR occured (errno=%d)", errno);
		(void) netclose(s);
		END_TRACE();
		return(-1);
	}
	unmarshall_LONG(p, status);
	TRACE(1, "rfio", "rfio_access: return %d",status);
	rfio_errno = status;
	(void) netclose(s);
	if (status)     {
		END_TRACE();
		return(-1);
	}
	END_TRACE();
	return (0);
}
