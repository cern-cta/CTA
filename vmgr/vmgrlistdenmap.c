/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrlistdenmap.c,v $ $Revision: 1.1 $ $Date: 2000/04/11 05:43:13 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*      vmgrlistdenmap - list triplets model/media_letter/density */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
#include "serrno.h"
#include "vmgr_api.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_denmap *lp;

        if (argc > 1) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s\n", argv[0]);
                exit (USERR);
        }
 
	flags = VMGR_LIST_BEGIN;
	while ((lp = vmgr_listdenmap (flags, &list)) != NULL) {
		printf ("%-6s %s %s\n", lp->md_model, lp->md_media_letter,
		    lp->md_density);
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listdenmap (VMGR_LIST_END, &list);
	exit (0);
}
