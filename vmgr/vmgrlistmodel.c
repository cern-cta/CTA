/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrlistmodel.c,v $ $Revision: 1.1 $ $Date: 2000/04/11 05:43:13 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*      vmgrlistmodel - list cartridge model entries */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
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
	struct vmgr_tape_media *lp;
	char tmpbuf[8];

        if (argc > 1) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s\n", argv[0]);
                exit (USERR);
        }
 
	flags = VMGR_LIST_BEGIN;
	while ((lp = vmgr_listmodel (flags, &list)) != NULL) {
		printf ("%-6s %s %-7s %d\n", lp->m_model, lp->m_media_letter,
		    u64tostru ((u_signed64)lp->native_capacity * ONE_MB, tmpbuf, 7),
		    lp->media_cost_GB);
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listmodel (VMGR_LIST_END, &list);
	exit (0);
}
