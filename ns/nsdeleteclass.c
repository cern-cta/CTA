/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nsdeleteclass.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:22 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	nsdeleteclass - delete a fileclass definition */
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "Cns_api.h"
#include "serrno.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
	int classid = 0;
	char *class_name = NULL;
	struct Cns_fileclass Cns_fileclass;
	char *dp;
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"id", REQUIRED_ARGUMENT, 0, OPT_CLASS_ID},
		{"name", REQUIRED_ARGUMENT, 0, OPT_CLASS_NAME},
		{0, 0, 0, 0}
	};
	char *server = NULL;

	memset (&Cns_fileclass, 0, sizeof(struct Cns_fileclass));
	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "h:", longopts, NULL)) != EOF) {
		switch (c) {
		case OPT_CLASS_ID:
			if ((classid = strtol (Coptarg, &dp, 10)) <= 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid classid %s\n", Coptarg);
				errflg++;
			} else
				Cns_fileclass.classid = classid;
			break;
		case OPT_CLASS_NAME:
			class_name = Coptarg;
			break;
		case 'h':
			server = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (Coptind < argc || (classid == 0 && class_name == NULL)) {
		errflg++;
	}
	if (errflg) {
		fprintf (stderr, "usage: %s %s", argv[0],
		    "--id classid --name class_name [-h name_server]\n");
		exit (USERR);
	}

	if (Cns_deleteclass (server, classid, class_name) < 0) {
		char buf[256];
		if (classid) sprintf (buf, "%d", classid);
		if (class_name) {
			if (classid) strcat (buf, ", ");
			else buf[0] = '\0';
			strcat (buf, class_name);
		}
		fprintf (stderr, "nsdeleteclass %s: %s\n", buf,
		    (serrno == ENOENT) ? "No such class" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
