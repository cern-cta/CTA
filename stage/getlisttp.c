/*
 * $Id: getlisttp.c,v 1.13 2001/11/30 11:29:37 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getlisttp.c,v $ $Revision: 1.13 $ $Date: 2001/11/30 11:29:37 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include "stage_constants.h"
#include "stage_messages.h"
#include "stage_macros.h"
#include "Cgetopt.h"

#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif

int getlist_of_vid _PROTO((char *, char[MAXVSN][7], int *));

int getlist_of_vid(opt, v, num)
		 char *opt;
		 char v[MAXVSN][7];
		 int *num;
{
	int errflg = 0;
	char *p;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

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
