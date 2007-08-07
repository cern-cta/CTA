/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      vmgrlistdgnmap - list triplets dgn/model/library */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "serrno.h"
#include "vmgr.h"
#include "vmgr_api.h"

int main(argc, argv)
int argc;
char **argv;
{
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_dgnmap *lp;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

        if (argc > 1) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s\n", argv[0]);
                exit (USERR);
        }
 
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, VMG52);
		exit (SYERR);
	}
#endif
	flags = VMGR_LIST_BEGIN;
	while ((lp = vmgr_listdgnmap (flags, &list)) != NULL) {
		printf ("%-6s %-6s %s\n", lp->dgn, lp->model, lp->library);
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listdgnmap (VMGR_LIST_END, &list);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}
