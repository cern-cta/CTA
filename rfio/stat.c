/*
 * $Id: stat.c,v 1.11 2005/02/22 13:28:52 jdurand Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stat.c,v $ $Revision: 1.11 $ $Date: 2005/02/22 13:28:52 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* stat.c       Remote File I/O - get file status                       */

#define RFIO_KERNEL 1
#include <rfio.h>


EXTERN_C int DLL_DECL rfio_smstat _PROTO((int, char *, struct stat *, int));

int  DLL_DECL rfio_stat(filepath, statbuf)       /* Remote file stat    */
char    *filepath;              /* remote file path                     */
struct stat *statbuf;           /* status buffer (subset of local used) */
{
#if (defined(__alpha) && defined(__osf__))
	return (rfio_stat64(filepath,statbuf));
#else
	int       status ;
#if defined(IRIX64) || defined(__ia64__) || defined(__x86_64) || defined(__ppc64__)
	struct stat64 statb64;

	if ((status = rfio_stat64(filepath,&statb64)) == 0)
		(void) stat64tostat(&statb64, statbuf);
	return (status);
#else
	register int    s;              /* socket descriptor            */
	char    *host, *filename;
	int 	rt, parserc ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_stat(%s, %x)", filepath, statbuf);

	if (!(parserc = rfio_parseln(filepath,&host,&filename,RDLINKS))) {
                /* if not a remote file, must be local or HSM  */
                if ( host != NULL ) {
                    /*
                     * HSM file
                     */
                    TRACE(1,"rfio","rfio_stat: %s is an HSM path",
                          filename);
                    END_TRACE();
                    rfio_errno = 0;
                    return(rfio_HsmIf_stat(filename,statbuf));
                }
		TRACE(1, "rfio", "rfio_stat: using local stat(%s, %x)",
			filename, statbuf);

		END_TRACE();
		rfio_errno = 0;
		status = stat(filename,statbuf);
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
	END_TRACE();
	status = rfio_smstat(s,filename,statbuf,RQST_STAT_SEC) ;
	if ( status == -1 && serrno == SEPROTONOTSUP ) {
		s = rfio_connect(host,&rt);
		if (s < 0)      {
			return(-1);
		}
		status = rfio_smstat(s,filename,statbuf,RQST_STAT) ;
	}
	(void) netclose(s);
	return (status);
#endif
#endif
}

int  DLL_DECL rfio_stat64(filepath, statbuf)     /* Remote file stat    */
char    *filepath;              /* remote file path                     */
struct stat64 *statbuf;         /* status buffer (subset of local used) */
{
	register int    s;              /* socket descriptor            */
	int       status ;
	char    *host, *filename;
	int 	rt,parserc ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_stat64(%s, %x)", filepath, statbuf);

	if (!(parserc = rfio_parseln(filepath,&host,&filename,RDLINKS))) {
                /* if not a remote file, must be local or HSM  */
                if ( host != NULL ) {
                    /*
                     * HSM file
                     */
                    TRACE(1,"rfio","rfio_stat64: %s is an HSM path", filename);
                    END_TRACE();
                    rfio_errno = 0;
                    return(rfio_HsmIf_stat64(filename,statbuf));
                }
		TRACE(1, "rfio", "rfio_stat64: using local stat64(%s, %x)",
			filename, statbuf);

		END_TRACE();
		rfio_errno = 0;
		status = stat64(filename,statbuf);
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
	END_TRACE();
	status = rfio_smstat64(s,filename,statbuf,RQST_STAT64) ;
	if ( status == -1 && serrno == SEPROTONOTSUP ) {
	  s = rfio_connect(host,&rt);
	  if (s < 0)      {
	    return(-1);
	  }
	  status = rfio_smstat64(s,filename,statbuf,RQST_STAT_SEC) ;
	  if ( status == -1 && serrno == SEPROTONOTSUP ) {
	    s = rfio_connect(host,&rt);
	    if (s < 0)      {
	      return(-1);
	    }
	    status = rfio_smstat64(s,filename,statbuf,RQST_STAT) ;
	  }
	}
	(void) netclose(s);
	return (status);
}
