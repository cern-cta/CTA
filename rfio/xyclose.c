/*
 * $Id: xyclose.c,v 1.5 2004/11/29 22:26:45 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: xyclose.c,v $ $Revision: 1.5 $ $Date: 2004/11/29 22:26:45 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* xyclose.c    Remote File I/O - Close a Fortran Logical Unit          */

/*
 * C bindings :
 *
 * rfio_xyclose(int lun, char *chopt, int *irc)
 *
 * FORTRAN bindings :
 *
 * XYCLOS(INTEGER*4 LUN, CHARACTER*(*) CHOPT, INTEGER*4 IRC)
 */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include "rfio.h"               /* remote file I/O definitions          */
#include <stdlib.h>

extern int DLL_DECL switch_close();

int DLL_DECL rfio_xyclose(lun, chopt, irc)   /* close a remote fortran logical unit  */
int     lun;
char    *chopt;
int     *irc;
{
	char    buf[LONGSIZE];  /* network command/response buffer      */
	register char *p=buf;   /* buffer pointer                       */
	int     status;         /* Fortran status                       */
	register int i;         /* general purpose index                */

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio" ,"rfio_xyclose(%d, %s, %x)", lun,  chopt, irc);
	if (ftnlun[lun] == (RFILE *) NULL)      { /* Allocated ?        */
		TRACE(1, "rfio", "rfio_xyclose: %s", "Bad file number");
		END_TRACE();
		return(EBADF);
	}

/*
 * Analyze options
 */

	TRACE(2, "rfio", "rfio_xyclose: parsing options: [%s]",chopt);
	for (i=0;i< (int)strlen(chopt);i++)   {
		switch (chopt[i])       {
			case ' ': break;
			default :
				*irc = SEBADFOPT;
				END_TRACE();
				return(SEBADFOPT);
		}
	}


	*irc = 0;

        if (!strcmp(ftnlun[lun]->host, "localhost"))      { /* Local file ?   */
                /* Must be a local file */
                *irc=switch_close(&lun);
                (void) free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
                END_TRACE();
		rfio_errno = 0;
                return(*irc);
        }

	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_XYCLOS);
	TRACE(2,"rfio","rfio_xyclose: writing %d bytes",RQSTSIZE) ;
	if (netwrite_timeout(ftnlun[lun]->s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
		TRACE(2, "rfio" ,"rfio_xyclose: write(): ERROR occured (errno=%d)", errno);
		free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
		END_TRACE();
		if ( serrno ) 	
			return ( serrno ) ;
		else
			return(errno);
	}
	p = buf;
	TRACE(2, "rfio" ,"rfio_xyclose: reading %d bytes", LONGSIZE);
	if (netread_timeout(ftnlun[lun]->s, buf, LONGSIZE, RFIO_CTRL_TIMEOUT) != LONGSIZE) {
		TRACE(2, "rfio" ,"rfio_xyclose: read(): ERROR occured (errno=%d)", errno);
		free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
		END_TRACE();
		if ( serrno ) 	
			return ( serrno ) ;
		else
			return( errno );
	}
	unmarshall_LONG(p, status);
	TRACE(1, "rfio", "rfio_xyclose: return %d ",status);
	END_TRACE();
	(void) close(ftnlun[lun]->s);
	free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
	*irc = status;
	rfio_errno = status ;
	return(status);
}

/*
 * Fortran wrapper
 */

#if defined(CRAY)
#include <fortran.h>            /* Fortran to C conversion macros       */
#endif /* CRAY */

#if defined(CRAY)
void XYCLOS(flun, fchopt, firc)
	int     *flun, *firc;
	_fcd    fchopt;
#else	/* sun || apollo || sgi || ultrix || AIX */
#if (defined(hpux) && !defined(PPU)) || (defined(_AIX) && defined(_IBMR2) && !defined(EXTNAME))
#define xyclos_		xyclos
#endif  /* hpux && !PPU || AIX && !EXTNAME */

#if defined(_WIN32)
void _stdcall XYCLOS(flun, fchopt, fchoptl, firc)
#else
void xyclos_(flun, fchopt, firc, fchoptl)
#endif
	int     *flun, *firc;
	char    *fchopt;
	int     fchoptl;
#endif  /* CRAY */
{
	char    *chopt;         /* xyclos options                       */
	int     status;         /* xyclos return status                 */
#if defined(CRAY)
	int     fchoptl;        /* CRAY fortran CHOPT string len        */
	char    *fchoptp;       /* CRAY fortran CHOPT C char pointer    */

	/*
	 * convert fortran arguments
	 */
	fchoptp = _fcdtocp(fchopt);
	fchoptl = _fcdlen(fchopt);
#endif  /* CRAY */

	INIT_TRACE("RFIO_TRACE");
	if ((chopt = malloc((unsigned) fchoptl+1)) == NULL)        {
		*firc = -errno;
		END_TRACE();
		return;
	}

#if defined(CRAY)
	strncpy(chopt, fchoptp, fchoptl); chopt[fchoptl] = '\0';
#else
	strncpy(chopt, fchopt, fchoptl); chopt[fchoptl] = '\0';
#endif  /* CRAY */

	TRACE(1, "rfio", "XYCLOS(%d, %s, %x)", *flun,  chopt, firc);
	status = rfio_xyclose(*flun, chopt, firc);
	if (status) *firc = -status;    /* system errors have precedence */
	TRACE(1, "rfio", "XYCLOS: status: %d, irc: %d",status,*firc);
	END_TRACE();
	(void) free(chopt);
	return;
}

#if defined(apollo)
void xyclos(flun, fchopt, firc, fchoptl)
	int     *flun, *firc;
	char    * fchopt ;
	short   * fchoptl ;
{
	xyclos_(flun, fchopt, firc, *fchoptl) ;
	return ; 
}
#endif	/* apollo */
