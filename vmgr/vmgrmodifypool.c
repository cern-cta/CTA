/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrmodifypool.c,v $ $Revision: 1.1 $ $Date: 2000/12/15 15:45:43 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrmodifypool - modify an existing tape pool definition */
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
	char *dp;
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"gid", REQUIRED_ARGUMENT, 0, OPT_POOL_GID},
		{"name", REQUIRED_ARGUMENT, 0, 'P'},
		{"uid", REQUIRED_ARGUMENT, 0, OPT_POOL_UID},
		{0, 0, 0, 0}
	};
	gid_t pool_gid = -1;
	char *pool_name = NULL;
	uid_t pool_uid = -1;

	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "P:", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_POOL_GID:
			if ((pool_gid = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid pool_gid %s\n", Coptarg);
				errflg++;
			}
			break;
		case OPT_POOL_UID:
			if ((pool_uid = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid pool_uid %s\n", Coptarg);
				errflg++;
			}
			break;
		case 'P':
			pool_name = Coptarg;
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
		    "--name pool_name [--gid pool_gid] [--uid pool_uid]\n");
                exit (USERR);
        }
 
	if (vmgr_modifypool (pool_name, pool_uid, pool_gid) < 0) {
		fprintf (stderr, "vmgrmodifypool %s: %s\n", pool_name, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
