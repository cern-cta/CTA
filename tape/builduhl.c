/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: builduhl.c,v $ $Revision: 1.3 $ $Date: 2007/03/23 13:08:33 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	builduhl - build UHL1 from tpmnt parameters */
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <time.h>
#include "Ctape.h"
#include "Ctape_api.h"
int builduhl(uhl1, fseq, blksize, lrecl, domainname, hostname, inq_data, drive_serial_no)
char uhl1[];
int fseq;
int blksize;
int lrecl;
char *domainname;
char *hostname;
char *inq_data;
char *drive_serial_no;
{
	int blkcnt;
	char buf[CA_MAXHOSTNAMELEN+1];
	int i;
	int n;
	char *p;

	blkcnt = 0;

	/* build UHL1 */

	for (i = 0; i < 80; i++)
		uhl1[i] = ' ';
	uhl1[80] = '\0';
	strcpy (uhl1, "UHL1");
	sprintf (buf, "%.10d", fseq);
	memcpy (uhl1 + 4, buf, 10);
	sprintf (buf, "%.10d", blksize);
	memcpy (uhl1 + 14, buf, 10);
	sprintf (buf, "%.10d", lrecl);
	memcpy (uhl1 + 24, buf, 10);
	strcpy (buf, domainname);
	if ((p = strchr (buf, '.'))) *p = '\0';
	UPPER (buf);
	memcpy (uhl1 + 34, buf, strlen (buf));
	strcpy (buf, hostname);
	UPPER (buf);
	memcpy (uhl1 + 42, buf, strlen (buf));
	if (*inq_data) {
		n = strlen (inq_data);
		memcpy (uhl1 + 52, inq_data, (n < 16) ? n : 16);
	}
	if (*drive_serial_no) {
		n = strlen (drive_serial_no);
		memcpy (uhl1 + 68, drive_serial_no, (n < 12) ? n : 12);
	}
        return (0);
}
