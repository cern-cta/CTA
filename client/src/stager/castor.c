/*
 * $Id: castor.c,v 1.4 2000/03/26 06:25:29 baud Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: castor.c,v $ $Revision: 1.4 $ $Date: 2000/03/26 06:25:29 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	castor - display current CASTOR version */
#include <stdio.h>
#include "patchlevel.h"
#define __BASEVERSION__ "?"
#define __PATCHLEVEL__ 0

main(argc, argv)
int argc;
char **argv;
{
	int c;
#if defined(HPSSCLIENT)
	char hpss_aware = 'H';
#else
	char hpss_aware = ' ';
#endif

	while ((c = getopt (argc, argv, "hv")) != EOF) {
		switch (c) {
		case 'v':
			printf ("%s.%d%c\n", BASEVERSION, PATCHLEVEL,
			    hpss_aware);
			break;
		case 'h':
		default:
			printf ("Please see documentation at http://wwwinfo.cern.ch/pdp/castor\n");
			break;
		}
	}
	exit (0);
}
