/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrenterlibrary.c,v $ $Revision: 1.1 $ $Date: 2001/03/08 15:22:16 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrenterlibrary - define a new tape library */
#include <grp.h>
#include <pwd.h>
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
	int capacity = 0;
	char *dp;
	int errflg = 0;
	char *library_name = NULL;
	static struct Coptions longopts[] = {
		{"capacity", REQUIRED_ARGUMENT, 0, OPT_CAPACITY},
		{"name", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
		{"status", REQUIRED_ARGUMENT, 0, OPT_STATUS},
		{0, 0, 0, 0}
	};
	int status = 0;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_CAPACITY:
			if ((capacity = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid capacity %s\n", Coptarg);
				errflg++;
			}
			break;
		case OPT_LIBRARY_NAME:
			library_name = Coptarg;
			break;
		case OPT_STATUS:
			if ((status = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid status %s\n", Coptarg);
				errflg++;
			}
			break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc) {
                errflg++;
        }
        if (errflg) {
		fprintf (stderr, "usage: %s %s", argv[0],
		    "--name library_name --capacity n [--status n]\n");
                exit (USERR);
        }
 
	if (vmgr_enterlibrary (library_name, capacity, status) < 0) {
		fprintf (stderr, "vmgrenterlibrary %s: %s\n", library_name,
		    sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
