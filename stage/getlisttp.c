/*
 * $Id: getlisttp.c,v 1.9 2000/12/12 14:13:40 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getlisttp.c,v $ $Revision: 1.9 $ $Date: 2000/12/12 14:13:40 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include "stage.h"
#include "Cgetopt.h"

getlist_of_vid(opt, v, num)
		 char *opt;
		 char v[MAXVSN][7];
		 int *num;
{
	int errflg = 0;
	char *p;

	if (*num == 0) {
		p = strtok (Coptarg, ":");
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
