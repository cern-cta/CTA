/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrmodifypool.c,v $ $Revision: 1.2 $ $Date: 2001/02/04 08:38:00 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrmodifypool - modify an existing tape pool definition */
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
	char *dp;
	int errflg = 0;
	struct group *gr;
	static struct Coptions longopts[] = {
		{"gid", REQUIRED_ARGUMENT, 0, OPT_POOL_GID},
		{"group", REQUIRED_ARGUMENT, 0, OPT_POOL_GROUP},
		{"name", REQUIRED_ARGUMENT, 0, 'P'},
		{"uid", REQUIRED_ARGUMENT, 0, OPT_POOL_UID},
		{"user", REQUIRED_ARGUMENT, 0, OPT_POOL_USER},
		{0, 0, 0, 0}
	};
	gid_t pool_gid = -1;
	char *pool_name = NULL;
	uid_t pool_uid = -1;
	struct passwd *pwd;

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
		case OPT_POOL_GROUP:
			if ((gr = getgrnam (Coptarg)) == NULL) {
				fprintf (stderr,
				    "invalid pool_group: %s\n", Coptarg);
				errflg++;
			} else
				pool_gid = gr->gr_gid;
			break;
		case OPT_POOL_UID:
			if ((pool_uid = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid pool_uid %s\n", Coptarg);
				errflg++;
			}
			break;
		case OPT_POOL_USER:
			if ((pwd = getpwnam (Coptarg)) == NULL) {
				fprintf (stderr,
				    "invalid pool_user: %s\n", Coptarg);
				errflg++;
			} else
				pool_uid = pwd->pw_uid;
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
		fprintf (stderr, "usage: %s %s %s", argv[0],
		    "--name pool_name [--gid pool_gid] [--group pool_group]\n",
		    "[--uid pool_uid] [--user pool_user]\n");
                exit (USERR);
        }
 
	if (vmgr_modifypool (pool_name, pool_uid, pool_gid) < 0) {
		fprintf (stderr, "vmgrmodifypool %s: %s\n", pool_name, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
