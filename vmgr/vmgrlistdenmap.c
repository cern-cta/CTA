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
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_api.h"

int main(int argc,
         char **argv)
{
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_denmap_byte_u64 *lp;
	char tmpbuf[80];

        if (argc > 1) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s\n", argv[0]);
                exit (USERR);
        }
 
	flags = VMGR_LIST_BEGIN;
	while ((lp = vmgr_listdenmap_byte_u64 (flags, &list)) != NULL) {
		printf ("%-6s %-2s %-8s %-8s\n", lp->md_model, lp->md_media_letter,
		    lp->md_density,
		    u64tostru ((u_signed64)lp->native_capacity_byte_u64, tmpbuf, 9));
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listdenmap_byte_u64 (VMGR_LIST_END, &list);
	exit (0);
}
