/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nsrmgrpmap.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:23 $ CERN IT-GD/SC Jean-Philippe Baud";
#endif /* not lint */

/*      nsrmgrpmap - suppress group entry corresponding to a given gid/name */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "Cns_api.h"
#include "serrno.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char *dp;
	int errflg = 0;
	gid_t gid = 0;
	char *groupname = NULL;
	static struct Coptions longopts[] = {
		{"gid", REQUIRED_ARGUMENT, 0, OPT_IDMAP_GID},
		{"group", REQUIRED_ARGUMENT, 0, OPT_IDMAP_GROUP},
		{0, 0, 0, 0}
	};

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
		switch (c) {
		case OPT_IDMAP_GID:
			if ((gid = strtol (Coptarg, &dp, 10)) < 0 || *dp != '\0') {
				fprintf (stderr, "invalid gid: %s\n", Coptarg);
				errflg++;
			}
			break;
		case OPT_IDMAP_GROUP:
			if (strlen (Coptarg) > 255) {
				fprintf (stderr,
				    "group name too long: %s\n", Coptarg);
				errflg++;
			} else
				groupname = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (Coptind < argc || (gid <= 0 && groupname == NULL))
		errflg++;
	if (errflg) {
		fprintf (stderr, "usage: %s %s", argv[0],
		    "--gid gid --group groupname\n");
		exit (USERR);
	}

	if (Cns_rmgrpmap (gid, groupname) < 0) {
		fprintf (stderr, "nsrmgrpmap %d: %s\n", gid,
		    (serrno == ENOENT) ? "No such group" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
