/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrlistmodel.c,v $ $Revision: 1.3 $ $Date: 2000/05/04 14:30:19 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*      vmgrlistmodel - list cartridge model entries */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"
#include "vmgr_api.h"
main(argc, argv)
int argc;
char **argv;
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
	char media_letter[2] = " ";
	char *model = NULL;
	int native_capacity;
	char tmpbuf[8];

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
		if (vmgr_querymodel (model, media_letter, &native_capacity,
		    &media_cost) < 0) {
			fprintf (stderr, "vmgrlistmodel %s: %s\n", model,
			    (serrno == ENOENT) ? "No such model" : sstrerror(serrno));
			exit (USERR);
		}
		printf ("%-6s %s %-7s %d\n", model, media_letter,
		    u64tostru ((u_signed64)native_capacity * ONE_MB, tmpbuf, 7),
		    media_cost);
	} else {
		flags = VMGR_LIST_BEGIN;
		while ((lp = vmgr_listmodel (flags, &list)) != NULL) {
			printf ("%-6s %s %-7s %d\n", lp->m_model, lp->m_media_letter,
			    u64tostru ((u_signed64)lp->native_capacity * ONE_MB, tmpbuf, 7),
			    lp->media_cost_GB);
			flags = VMGR_LIST_CONTINUE;
		}
		(void) vmgr_listmodel (VMGR_LIST_END, &list);
	}
	exit (0);
}
