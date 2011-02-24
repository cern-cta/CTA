/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrmodifypool - modify an existing tape pool definition */
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cgetopt_trunk_r21843.h"
#include "serrno_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"

int main(int argc,
         char **argv)
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

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "P:", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_POOL_GID:
      pool_gid = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
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
      pool_uid = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
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
		fprintf (stderr, "vmgrmodifypool %s: %s\n", pool_name, 
			 sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
