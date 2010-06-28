/*
 * Copyright (C) 2003 by CERN/IT/GD/CT
 * All rights reserved
 */

/*	vmgrsettag - add/replace a tag associated with a tape volume */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "vmgr.h"
#include "vmgr_api.h"

int main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"tag", REQUIRED_ARGUMENT, 0, OPT_TAG},
		{0, 0, 0, 0}
	};
	char *tag = NULL;
	char *vid = NULL;

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "V:", longopts, NULL)) != EOF) {
		switch (c) {
		case 'V':
			vid = Coptarg;
			break;
		case OPT_TAG:
			tag = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (Coptind < argc || vid == NULL || tag == NULL) {
		fprintf (stderr,
		    "usage: %s --tag text -V vid\n", argv[0]);
		exit (USERR);
	}
	if (vmgr_settag (vid, tag)) {
		fprintf (stderr, "%s: %s\n", vid, sstrerror(serrno));
		errflg++;
	}
	if (errflg)
		exit (USERR);
	exit (0);
}
