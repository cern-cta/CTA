/*
 * $Id: getlisttp.c,v 1.7 2000/02/11 11:06:49 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getlisttp.c,v $ $Revision: 1.7 $ $Date: 2000/02/11 11:06:49 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include "stage.h"
extern char *optarg;

getlist_of_vid(opt, v, num)
char *opt;
char v[MAXVSN][7];
int *num;
{
	int errflg = 0;
	char *p;

	if (*num == 0) {
		p = strtok (optarg, ":");
		while (p != NULL) {
			if (*num >= MAXVSN) {
				fprintf (stderr, STG14);
				errflg++;
				break;
			}
			if ((int) strlen (p) > 0 && (int) strlen (p) < 7) {
				strcpy (v[*num], p);
				UPPER (v[*num]);
			} else {
				fprintf (stderr, STG06, opt);
				errflg++;
			}
			(*num)++;
			if ((p = strtok (NULL, ":")) != NULL) *(p - 1) = ':';
		}
	} else {
		fprintf (stderr, STG13, opt);
		errflg++;
	}
	return (errflg);
}
