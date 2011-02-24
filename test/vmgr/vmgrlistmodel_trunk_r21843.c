/*
 * Copyright (C) 2000-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      vmgrlistmodel - list cartridge model entries */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cgetopt_trunk_r21843.h"
#include "serrno_trunk_r21843.h"
#include "vmgr_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"

int main(int argc,
         char **argv)
{
	int c;
	int errflg = 0;
	int flags;
	vmgr_list list;
	static struct Coptions longopts[] = {
		{"media_letter", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"ml", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"mo", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{0, 0, 0, 0}
	};
	struct vmgr_tape_media *lp;
	int media_cost;
	char media_letter[CA_MAXMLLEN+1] = " ";
	char *model = NULL;

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
		switch (c) {
		case OPT_MEDIA_LETTER:
			strcpy (media_letter, Coptarg);
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
	if (Coptind < argc) {
		errflg++;
	}
	if (errflg) {
		fprintf (stderr, "usage: %s %s", argv[0],
		    "[--mo model] [--ml media_letter]\n");
		exit (USERR);
	}
 
	if (model) {
		if (vmgr_querymodel (model, media_letter, &media_cost) < 0) {
			fprintf (stderr, "vmgrlistmodel %s: %s\n", model,
			    (serrno == ENOENT) ? "No such model" : sstrerror(serrno));
			exit (USERR);
		}
		printf ("%-6s %-2s %d\n", model, media_letter, media_cost);
	} else {
		flags = VMGR_LIST_BEGIN;
		while ((lp = vmgr_listmodel (flags, &list)) != NULL) {
			printf ("%-6s %-2s %d\n", lp->m_model, lp->m_media_letter,
			    lp->media_cost);
			flags = VMGR_LIST_CONTINUE;
		}
		(void) vmgr_listmodel (VMGR_LIST_END, &list);
	}
	exit (0);
}
