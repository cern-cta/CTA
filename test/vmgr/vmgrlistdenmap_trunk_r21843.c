/*
 * Copyright (C) 2000-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      vmgrlistdenmap - list quadruplets model/media_letter/density/capacity */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include "serrno_trunk_r21843.h"
#include "u64subr_trunk_r21843.h"
#include "vmgr_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"

int main(int argc,
         char **argv)
{
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_denmap *lp;
	char tmpbuf[80];

        if (argc > 1) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s\n", argv[0]);
                exit (USERR);
        }
 
	flags = VMGR_LIST_BEGIN;
	while ((lp = vmgr_listdenmap (flags, &list)) != NULL) {
		printf ("%-6s %-2s %-8s %-8s\n", lp->md_model, lp->md_media_letter,
		    lp->md_density,
		    u64tostru ((u_signed64)lp->native_capacity * ONE_MB, tmpbuf, 9));
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listdenmap (VMGR_LIST_END, &list);
	exit (0);
}
