/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: segment.c,v $ $Revision: 1.3 $ $Date: 2000/05/31 10:33:53 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* header.c     Streams headers for Ultranet BUG                        */

/* changed by   date               description                          */
/*+-----------+-----------+--------------------------------------------+*/
/* F. Hemmer     7 Mar 91       Initial writing                         */

#ifdef MINBLOCKSIZE             /* Minimum blocksize is required        */

#define DEBUG   0               /* Debugging flag                       */
 
#include <stdio.h>              /* Standard Input/Output                */
#include <errno.h>              /* Error numbers and codes              */
#include <sys/types.h>          /*         Standard data types          */
#include <netinet/in.h>         /*         Internet data types          */
#include <marshall.h>           /* Marshalling macros                   */

#define READ(x,y,z)     s_recv(x,y,z)   /* read function to use         */
#define WRITE(x,y,z)    s_send(x,y,z)   /* write function to use        */

static  char ibuffer[MINBLOCKSIZE];     /* Internal buffer              */

#define SMALLBLOCK      0x01770101      /* SMALL BLOCK code             */
#define LARGEBLOCK      0x77770101      /* LARGE BLOCK code             */

static  char    *rbp=0;                 /* Receive buffer pointer       */
static  int     rbs=0;                  /* Receive buffer size          */

int DLL_DECL 
seg_send (s, buf, nbytes)
int     s;
char    *buf;
int     nbytes;
{
	LONG    hdrtype;        /* header type field                    */
	LONG    hdrlen;         /* header block length field            */
	char    *p;             /* buffer pointer                       */

#if DEBUG
	fprintf(stdout,"@(#)segment.c	1.4: dosend(%d, %x, %d)\n",s, buf, nbytes);
#endif /* DEBUG */
	p = ibuffer;
	if (nbytes <= MINBLOCKSIZE)     {
		hdrtype = SMALLBLOCK;
		hdrlen  = nbytes;
		marshall_LONG(p, hdrtype);
		marshall_LONG(p, hdrlen);
#if DEBUG
		fprintf(stdout,"@(#)segment.c	1.4: sending header (SMALLBLOCK,%d)\n",hdrlen);
#endif /* DEBUG */
	}
	else    {
		hdrtype = LARGEBLOCK;
		hdrlen  = nbytes;
		marshall_LONG(p, hdrtype);
		marshall_LONG(p, hdrlen);
#if DEBUG
		fprintf(stdout,"@(#)segment.c	1.4: sending header (LARGEBLOCK,%d)\n",hdrlen);
#endif /* DEBUG */
	}
	if (WRITE(s, ibuffer, MINBLOCKSIZE) != MINBLOCKSIZE) {
#if DEBUG
		fprintf(stdout,"@(#)segment.c	1.4: WRITE error:\n", errno);
#endif /* DEBUG */
		return(-1);
	}
	if (hdrtype == LARGEBLOCK)      {
#if DEBUG
		fprintf(stdout,"WRITE LARGE DATA BLOCK\n");
#endif /* DEBUG */
		return(WRITE(s, buf, nbytes));
	}
	else    {
#if DEBUG
		fprintf(stdout,"@(#)segment.c	1.4: SEGMENTATION, copy %d bytes, WRITE DATA BLOCK\n", nbytes);
#endif /* DEBUG */
		memcpy(ibuffer, buf, nbytes);
		if (WRITE(s, ibuffer, MINBLOCKSIZE) != MINBLOCKSIZE) {
#if DEBUG
			fprintf(stdout,"@(#)segment.c	1.4: WRITE error:\n", errno);
#endif /* DEBUG */
			return(-1);
		}
	return (nbytes);
	}
}

int DLL_DECL
seg_recv (s, buf, nbytes)
int     s;
char    *buf;
int     nbytes;
{
	char    *ubp = buf;     /* User buffer pointer          */
	int     unb = nbytes;   /* User buffer byte count       */
	LONG    hdrtype;        /* header type field                    */
	LONG    hdrlen;         /* header block length field            */
	char    *p;             /* buffer pointer                       */

	if (nbytes == 0) return(0);
#if DEBUG
	fprintf(stdout,"@(#)segment.c	1.4: dorecv(%d, %x, %d)\n",s, buf, nbytes);
#endif /* DEBUG */
	if (rbs >= nbytes)    {
		memcpy(ubp, rbp, nbytes);
		rbp += nbytes;
		rbs -= nbytes;
		return (nbytes);
	}
	else    {
#if DEBUG
		fprintf(stdout,"@(#)segment.c	1.4: COPYING %d bytes (left buffer)\n", rbs);
#endif /* BUFFER */
		if (rbs > 0)    {
			memcpy(ubp, rbp, rbs);
			unb -= rbs;
		}
		rbs = 0;
#if DEBUG
		fprintf(stdout,"@(#)segment.c	1.4: READ header\n");
#endif /* DEBUG */
			if (READ(s, ibuffer, MINBLOCKSIZE) != MINBLOCKSIZE)     {
#if DEBUG
			fprintf(stdout,"@(#)segment.c	1.4: READ error:\n", errno);
#endif /* DEBUG */
			return(-1);
		}
		p = ibuffer;
		unmarshall_LONG(p, hdrtype);
		unmarshall_LONG(p, hdrlen);
		if (hdrtype == LARGEBLOCK)      {
#if DEBUG
			fprintf(stdout,"@(#)segment.c	1.4: LARGEBLOCK received\n");
#endif /* DEBUG */
			return (READ(s, ubp, unb));
		}
		else    {
#if DEBUG
			fprintf(stdout,"@(#)segment.c	1.4: SMALLBLOCK received\n");
#endif /* DEBUG */
			if (READ(s, ibuffer, MINBLOCKSIZE) != MINBLOCKSIZE)     {
#if DEBUG
				fprintf(stdout,"@(#)segment.c	1.4: READ error:\n", errno);
#endif /* DEBUG */
				return(-1);
			}
		}
		rbs = hdrlen;
		rbp = ibuffer;
/*
 * recursive call
 */
		return(dorecv(s, ubp, unb));

	}
}
#endif /* MINBLOCKSIZE */
