/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrenterdgnmap.c,v $ $Revision: 1.1 $ $Date: 2001/03/08 15:22:19 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrenterdgnmap - enter a new triplet dgn/model/library */
#include <stdio.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "vmgr_api.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char *dgn = NULL;
	int errflg = 0;
	char *library_name = NULL;
	static struct Coptions longopts[] = {
		{"library", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{0, 0, 0, 0}
	};
	char *model = NULL;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "g:", longopts, NULL)) != EOF) {
                switch (c) {
                case 'g':
			dgn = Coptarg;
                        break;
                case OPT_LIBRARY_NAME:
			library_name = Coptarg;
                        break;
                case OPT_MODEL:
			model = Coptarg;
                        break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc || dgn == NULL || model == NULL || library_name == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s", argv[0],
		    "-g dgn --library library_name --mo model\n");
                exit (USERR);
        }
 
	if (vmgr_enterdgnmap (dgn, model, library_name) < 0) {
		fprintf (stderr, "vmgrenterdgnmap %s: %s\n", dgn, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
