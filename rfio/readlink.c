/*
 * $Id: readlink.c,v 1.3 1999/12/09 08:48:10 baran Exp $
 *
 * $Log: readlink.c,v $
 * Revision 1.3  1999/12/09 08:48:10  baran
 * Thread-safe version
 *
 * Revision 1.2  1999/07/20 12:48:08  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */


/*
 * Copyright (C) 1994-1997 by CERN CN-PDP/CS
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)readlink.c	1.6 5/6/98 CERN CN-PDP/CS F. Hassine";
#endif /* not lint */

#define RFIO_KERNEL     1
#include "rfio.h"

#ifndef linux
extern char *sys_errlist[];     /* system error list */
#endif

int rfio_readlink(path,buf, length)
char *path ;
char *buf ;
int length ;

{
	int c;
        int status ;
	int s ;
	char *host ;
	char * filename;
	char *p ;
	int rt ;
	int rcode , len;
	int uid ;
	int gid ;
	static  char buffer[256];

/*
         * The file is local.
         */
        INIT_TRACE("RFIO_TRACE");
	TRACE( 1, "rfio", " rfio_readlink (%s,%x,%d)",path,buf,length);
        if ( ! rfio_parseln(path,&host,&filename,NORDLINKS) ) {
#if !defined(_WIN32)
		status = readlink(filename,buf,length) ;
#else
		{ serrno = SEOPNOTSUP; status = -1; }
#endif
                END_TRACE() ;
		rfio_errno = 0;
                return status ;
        }

        s = rfio_connect(host,&rt);
        if (s < 0)      {
                END_TRACE();
                return(-1);
        }
	uid = geteuid() ;
	gid = getegid () ;

        p = buffer ;
        marshall_WORD(p, B_RFIO_MAGIC);
        marshall_WORD(p, RQST_READLINK);

	status = 2*WORDSIZE + strlen(path) + 1;
	marshall_LONG(p, status) ;

        if (netwrite_timeout(s,buffer,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
                TRACE(2, "rfio", "readlink: write(): ERROR occured (errno=%d)",errno);
                (void) close(s);
                END_TRACE();
                return(-1);
        }

	p = buffer ;
	marshall_WORD(p,uid) ;
	marshall_WORD(p,gid) ;
	marshall_STRING(p,filename) ;
	
	if (netwrite_timeout(s,buffer,status,RFIO_CTRL_TIMEOUT) != status ) {
                TRACE(2, "rfio", "readlink(): write(): ERROR occured (errno=%d)",errno);
                (void) close(s);
                END_TRACE();
                return(-1);
        }

	/*
 	 * Getting back status
	 */ 
        if ((c=netread_timeout(s, buffer, 3*LONGSIZE, RFIO_CTRL_TIMEOUT)) != (3*LONGSIZE))  {
		if (c == 0) {
			serrno = SEOPNOTSUP;    /* symbolic links not supported on remote machine */
			TRACE(2, "rfio", "rfio_readlink: read(): ERROR occured (serrno=%d)", serrno);
		} else
			TRACE(2, "rfio", "rfio_readlink: read(): ERROR occured (errno=%d)", errno);
                (void) close(s);
                END_TRACE();
                return(-1);
        }
	p = buffer;
	unmarshall_LONG( p, len );
        unmarshall_LONG( p, status ) ;
	unmarshall_LONG( p, rcode ) ;

        if ( status < 0 ) {
		TRACE(1,"rfio","rfio_readlink(): rcode = %d , status = %d",rcode, status);
		rfio_errno = rcode ;
                (void) close(s);
                END_TRACE();
                return(status);
        }
        /* Length is not of a long size, so RFIO_CTRL_TIMEOUT is enough */
        if (netread_timeout(s, buffer, len, RFIO_CTRL_TIMEOUT) != len)  {
                TRACE(2, "rfio", "rfio_readlink: read(): ERROR occured (errno=%d)", errno);
                (void) close(s);
                END_TRACE();
                return(-1);
        }
	p = buffer ;
	unmarshall_STRING( p, buf ) ;
	TRACE (2,"rfio","rfio_readlink succeded: returned %s",buf);
	END_TRACE();
	(void) close (s) ;
	return(rcode) ;
}

