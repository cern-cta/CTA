/*
 * $Id: switch_req.c,v 1.2 1999/07/20 12:48:33 jdurand Exp $
 *
 * $Log: switch_req.c,v $
 * Revision 1.2  1999/07/20 12:48:33  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990-1998 by CERN/CN/SW/DC
 * All rights reserved
 *
 * This is a file of functions that merge some code common
 * to rfio_fcalls.c and xyopen.c, xyclose.c, xywrite.c and  xyread.c
 *
 */

#define DEBUG           0               /* Debugging flag               */
#define RFIO_KERNEL     1               /* KERNEL part of the programs  */

#ifndef lint
static char sccsid[] = "@(#)switch_req.c	1.11 06/05/98  CERN CN-SW/DC F. Hassine";
#endif

#include "rfio.h"                       /* Remote file I/O              */
#include "osdep.h"
#include <log.h>                        /* Genralized error logger      */

#if defined(CRAY)
#define fopn_us_	FOPN_US
#define fopn_ud_	FOPN_UD
#define fwr_us_		FWR_US
#define fwr_ud_		FWR_UD
#define frd_us_		FRD_US
#define frd_ud_		FRD_UD
#define fcls_f_		FCLS_F
#define frdc_		FRDC
#endif  /* CRAY */

int switch_open(access, lun, filename, filen, lrecl,append,trunc,mod)
int     *access		;
LONG	*lun		;
char 	*filename	;
int	*filen		;
LONG 	*lrecl		;	
LONG  	*append		;
LONG	*trunc		;
int 	mod		;
{
	int status;
        switch ((int)*access) {
                        case FFFACC_S:
				if (mod == LLTM) 
                                	log(LOG_INFO, "rxyopen(%s) SEQUENTIAL\n",filename);
				else 
					TRACE(2, "rfio",  "rfio_xyopen(%s) SEQUENTIAL (local)",filename);

#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) ) || ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)

                                status=usf_open(lun, filename, append,trunc);
#else
                                (void) fopn_us_(lun, filename, filen, append, &status);
#endif
                                break;
                        case FFFACC_D:
				if (mod == LLTM)
                                	log(LOG_INFO, "rxyopen(%s) DIRECT\n",filename);
				else {
					TRACE(2, "rfio",  "rfio_xyopen(%s) DIRECT (local)",filename);
				}

#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) ) || ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)
                                status=udf_open(lun, filename, lrecl,trunc);
#else
                                (void) fopn_ud_(lun, filename, filen, lrecl, &status);
#endif
                                break;
                        default:
				if (mod == LLTM)
                                	log(LOG_ERR, "rxyopen(%s) invalid access type: %d\n",filename, *access);
				else {
					TRACE(2, "rfio",  "rfio_xyopen(%d) invalid access type: %d\n",lun, *access);
				}
                                status = SEBADFOPT;
        }
		return (status);

}

int switch_write(access,lun,ptr,nwrit,nrec,mod)
int     access          ;
LONG    *lun            ;
char 	*ptr		;
int 	*nwrit		;
int	*nrec		;
int 	mod 		;
{ 
	int status;
	switch (access) {
                case FFFACC_S:
			 if (mod == LLTM)
                        	log(LOG_DEBUG, "rxywrit(%d) SEQUENTIAL\n",*lun);
			 else {
				TRACE(2, "rfio",  "rfio_xywrit(%d) SEQUENTIAL",*lun);
			 }

#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) ) || ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)
                        status=usf_write(lun, ptr, nwrit);
#else
                        (void) fwr_us_(lun, ptr, nwrit, &status);
#endif
                        break;
                case FFFACC_D:
			if (mod == LLTM)
                        	log(LOG_DEBUG, "rxywrit(%d) DIRECT\n",*lun);
			else
				TRACE(2, "rfio",  "rfio_xywrit(%d) DIRECT",*lun);

#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) )|| ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)
                        status=udf_write(lun, ptr, nrec, nwrit);
#else
                        (void) fwr_ud_(lun, ptr, nrec, nwrit, &status);
#endif
                        break;
                default:
			if (mod == LLTM)
                        	log(LOG_ERR, "rxyopen(%d) invalid access type: %d\n",*lun, access);
			else
				TRACE(2, "rfio",  "rfio_xywrite(%d) invalid access type:%d",*lun, access);

                        status = SEBADFOPT;
        }
	return (status);
}


int switch_read(access,ptlun,buffer1,nwant,nrec,readopt,ngot,mod)
int  	access	;
int 	*ptlun  	;
char 	*buffer1;
int	*nwant	;
int 	*nrec	;
int	readopt	;
int	*ngot	;
int 	mod	;

{
	int status;
        if (readopt == FFREAD_C)        {
			if (mod == LLTM)
                                log(LOG_DEBUG, "rxyread(%d) SPECIAL\n",*ptlun);
			else {
				TRACE(2, "rfio", "rfio_xyread(%d) SPECIAL",*ptlun);
			}

#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) ) || ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)
                                (void) uf_cread(ptlun, buffer1, nrec, nwant, ngot, &status);
#else
                                (void) frdc_(ptlun, buffer1, nwant, ngot, &status);
#endif

        }
        else    {
                switch (access) {
                        case FFFACC_S:
				if (mod == LLTM)
                                	log(LOG_DEBUG, "rxyread(%d) SEQUENTIAL\n",*ptlun);
				else
					TRACE(2, "rfio", "rfio_xyread(%d) SEQUENTIAL",*ptlun);
#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) ) || ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)
                                status=usf_read(ptlun, buffer1, nwant);
#else
                                (void) frd_us_(ptlun, buffer1, nwant, &status);
#endif

                                *ngot = *nwant;
                                break;
                        case FFFACC_D:
				if (mod == LLTM)
                                	log(LOG_DEBUG, "rxyread(%d) DIRECT\n",*ptlun);
				else
					TRACE(2, "rfio", "rfio_xyread(%d) DIRECT",*ptlun);
#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) ) || ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)
                                status =udf_read(ptlun, buffer1, nrec, nwant);
#else
                                (void) frd_ud_(ptlun, buffer1, nrec, nwant, &status);
#endif

                                *ngot = *nwant;
                                break;
                        default:
				if (mod == LLTM)
                                	log(LOG_ERR, "rxyread(%d) invalid access type: %d\n",*ptlun, access);
				else
					 TRACE(2, "rfio", "rfio_xyread(%d) invalid access type: %d",*ptlun, access);
                                *ngot = 0;
                                status = SEBADFOPT;
                }
	}
	return (status);

}

int switch_close(lun)
int	*lun;
{
	int irc;
#if defined(sun) || defined(apollo) || defined(sgi) || defined(hpux) || defined(_AIX) || (defined(ultrix) && defined(mips) ) || ( defined(__osf__) && defined(__alpha) ) || defined(linux) || defined(_WIN32) || defined(__Lynx__)
        irc=uf_close(lun);
#else
        (void) fcls_f_ (lun , &irc) ;
#endif
	return(irc);
}
