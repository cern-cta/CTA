/*
 * $Id: stat.c,v 1.3 1999/12/09 13:47:18 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stat.c,v $ $Revision: 1.3 $ $Date: 1999/12/09 13:47:18 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* stat.c       Remote File I/O - get file status                       */

#define RFIO_KERNEL 1
#include <rfio.h>


int  rfio_stat(filepath, statbuf)       /* Remote file stat             */
char    *filepath;              /* remote file path                     */
struct stat *statbuf;           /* status buffer (subset of local used) */
{
	register int    s;              /* socket descriptor            */
	int       status ;
	char    *host, *filename;
	int 	rt ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_stat(%s, %x)", filepath, statbuf);

	if (!rfio_parseln(filepath,&host,&filename,RDLINKS)) {
		/* if not a remote file, must be local  */
#if LOCAL_IO
		TRACE(1, "rfio", "rfio_fopen: using local stat(%s, %x)",
			filename, statbuf);

		END_TRACE();
		rfio_errno = 0;
		return(stat(filename,statbuf));
#else
		END_TRACE();
		rfio_errno = SENOTRFILE;
		return(-1);
#endif /* LOCAL_IO */
	}

	s = rfio_connect(host,&rt);
	if (s < 0)      {
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
	(void) close(s);
	return (status);
}
