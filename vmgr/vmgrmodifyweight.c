/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrmodifyweight.c,v $ $Revision: 1.1 $ $Date: 2005/03/17 10:23:36 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrmodifyweight - modify the current weight of a given dgn */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "vmgr_api.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
        char *dp;
	int errflg = 0;
	char *dgn = NULL;
	static struct Coptions longopts[] = {
		{"dgn", REQUIRED_ARGUMENT, 0, OPT_DGN },
		{"weight", REQUIRED_ARGUMENT, 0, OPT_WEIGHT},
		{"delta", REQUIRED_ARGUMENT, 0, OPT_DELTA},
		{0, 0, 0, 0}
	};
	int weight = -1;
	int delta = 0;
	int valid = 0;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "d:w:", longopts, NULL)) != EOF) {
                switch (c) {
		case 'd':
		case OPT_DGN:
			dgn = Coptarg;
			break;
		case 'w':
		case OPT_WEIGHT:
                        if ((weight = strtol (Coptarg, &dp, 10)) < 0 ||
                                    *dp != '\0') {

				fprintf (stderr,
				   "invalid weight%s\n", Coptarg);
				errflg++;
			}
			valid = valid | OPT_VALID_WEIGHT;
                        break;
                case OPT_DELTA:
                        if ((delta = strtol (Coptarg, &dp, 10)) < 0 ||
                                    *dp != '\0') {
                                                                                
                                fprintf (stderr,
                                   "invalid delta%s\n", Coptarg);
                                errflg++;
                        }
			valid = valid | OPT_VALID_DELTA;
                        break;

                default:
			printf("Undefined option\n");
                        break;
                }
        }
        if (Coptind < argc) {
                errflg++;
        }
        /* DGN arguments is mandatory, and either weight, delta, or both
           should also be provided. Check that they are passed */
        if ((dgn == NULL) || (valid == 0)) {
		errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s", argv[0],
		    "--dgn dgn [--weight weight | --delta delta]\n");
                exit (USERR);
        }
 
        /* valid specifies wich parameters have valid data */
	if (vmgr_modifyweight (dgn, weight, delta, valid) < 0) {
		fprintf (stderr, "vmgrmodifyweight %s: %s\n", dgn, 
		    (serrno == ENOENT) ? "No such dgn" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
