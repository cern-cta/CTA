/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: buildhdrlbl.c,v $ $Revision: 1.5 $ $Date: 2007/03/06 16:46:35 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	buildhdrlbl - build HDR1 and HDR2 from tpmnt parameters */
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <time.h>
#include "patchlevel.h"
#define __BASEVERSION__ "?"
#include "Ctape.h"
int buildhdrlbl(hdr1, hdr2, fid, fsid, fsec, fseq, retentd, recfm, blksize, lrecl, den, lblcode)
char hdr1[];
char hdr2[];
char *fid;
char *fsid;
int fsec;
int fseq;
int retentd;
char *recfm;
int blksize;
int lrecl;
int den;
int lblcode;
{
	int blkcnt;
	char buf[7];
	time_t current_time;
	int i;
	struct tm *localtime(), *tm;

	blkcnt = 0;

	/* build HDR1 */

	for (i = 0; i < 80; i++)
		hdr1[i] = ' ';
	hdr1[80] = '\0';
	strcpy (hdr1, "HDR1");
	memcpy (hdr1 + 4, fid, strlen (fid));
	memcpy (hdr1 + 21, fsid, strlen (fsid));
	sprintf (buf, "%.4d", fsec);
	memcpy (hdr1 + 27, buf, 4);
	sprintf (buf, "%.4d", fseq % 10000);
	memcpy (hdr1 + 31, buf, 4);
	memcpy (hdr1 + 35, "0001", 4);
	memcpy (hdr1 + 39, "00", 2);
	time (&current_time);
	tm = localtime (&current_time);
	sprintf (buf, "%c%.2d%.3d", tm->tm_year / 100 ? '0' : ' ',
		tm->tm_year % 100, tm->tm_yday + 1);
	memcpy (hdr1 + 41, buf, 6);
	current_time += (retentd * 86400);
	tm = localtime (&current_time);
	sprintf (buf, "%c%.2d%.3d", tm->tm_year / 100 ? '0' : ' ',
		tm->tm_year % 100, tm->tm_yday + 1);
	memcpy (hdr1 + 47, buf, 6);
	sprintf (buf, "%.6d", blkcnt);
	memcpy (hdr1 + 54, buf, 6);
	memcpy (hdr1 + 60, "CASTOR", 6);
	memcpy (hdr1 + 67, BASEVERSION, strlen (BASEVERSION));

	/* build HDR2 */

	for (i = 0; i < 80; i++)
		hdr2[i] = ' ';
	hdr2[80] = '\0';
	strcpy (hdr2, "HDR2");
	hdr2[4] = *recfm;
	if (lblcode == SL && *(recfm+1)) {
		if (strcmp (recfm + 1, "BS") == 0)
			hdr2[38] = 'R';
		else
			hdr2[38] = *(recfm+1);
	}
	if (blksize < 100000) {
		sprintf (buf, "%.5d", blksize);
		memcpy (hdr2 + 5, buf, 5);
	} else
		memcpy (hdr2 + 5, "00000", 5);
	if (lrecl < 100000) {
		sprintf (buf, "%.5d", lrecl);
		memcpy (hdr2 + 10, buf, 5);
	} else
		memcpy (hdr2 + 10, "00000", 5);
	if (den == D800) hdr2[15] = '2';
	else if (den == D1600) hdr2[15] = '3';
	else if (den == D6250) hdr2[15] = '4';
	else if (den & IDRC) hdr2[34] = 'P';
	if (lblcode == AL || lblcode == AUL)
		memcpy (hdr2 + 50, "00", 2);
        return (0);
}
