/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrlistlibrary - query a given library or list all existing tape libraries */
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
#include "vmgr_constants.h"

void listentry(char *library_name,
               int capacity,
               int nb_free_slots,
               int status)
{
	const char *statusStr = NULL;


	switch(status) {
	case LIBRARY_ONLINE:
		statusStr = "ONLINE";
		break;
	case LIBRARY_OFFLINE:
		statusStr = "OFFLINE";
		break;
	default:
		statusStr = "UNKNOWN";
	}
	printf ("%-8s CAPACITY %d FREE %d (%5.1f%%) %s\n",
		library_name, capacity, nb_free_slots,
		capacity ? (float)nb_free_slots * 100. / (float)capacity : 0,
		statusStr);
}

int main(int argc,
         char **argv)
{
	int c;
	int capacity;
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_library *lp;
	char *library_name = NULL;
	static struct Coptions longopts[] = {
		{"name", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
		{0, 0, 0, 0}
	};
	int nb_free_slots;
	int status;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_LIBRARY_NAME:
			library_name = Coptarg;
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
                fprintf (stderr, "usage: %s %s", argv[0], "[--name library_name]\n");
                exit (USERR);
        }
 
	if (library_name) {
		if (vmgr_querylibrary (library_name, &capacity, &nb_free_slots,
		    &status) < 0) {
			fprintf (stderr, "vmgrlistlibrary %s: %s\n", library_name,
			    (serrno == ENOENT) ? "No such library" : sstrerror(serrno));
			exit (USERR);
		}
		listentry (library_name, capacity, nb_free_slots, status);
	} else {
		flags = VMGR_LIST_BEGIN;
		while ((lp = vmgr_listlibrary (flags, &list)) != NULL) {
			listentry (lp->name, lp->capacity, lp->nb_free_slots,
			    lp->status);
			flags = VMGR_LIST_CONTINUE;
		}
		(void) vmgr_listlibrary (VMGR_LIST_END, &list);
	}
	exit (0);
}
