/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrlistweight.c,v $ $Revision: 1.1 $ $Date: 2005/03/17 10:23:36 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrlistweight - query a given dgn for its weight or list all existing dgns for its weights */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_api.h"
extern	char	*optarg;
extern	int	optind;
main(argc, argv)
int argc;
char **argv;
{
	int c;
	u_signed64 capacity;
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_dgn *lp;
	char *dgn = NULL;
	int weight;
        int delta;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

        while ((c = getopt (argc, argv, "d:")) != EOF) {
                switch (c) {
		case 'd':
			dgn = optarg;
                        break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (optind < argc) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s", argv[0], "[-d dgn]\n");
                exit (USERR);
        }
 
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, VMG52);
		exit (SYERR);
	}
#endif
	if (dgn) {
		/* we were specified a dgn, give only the weight for the 
		   given dgn
		*/
		if (vmgr_queryweight (dgn, &weight, &delta) < 0 ){
			fprintf (stderr, "vmgrlistweight %s: %s\n", dgn, 
			    (serrno == ENOENT) ? "No such dgn" : sstrerror(serrno));
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (USERR);
		}
		listentry (dgn, weight, delta);
	} else {
		/* we were not asked for an specific dgn, list the weight
		   for all existing dgns
		*/
		print_header();
		flags = VMGR_LIST_BEGIN;
		while ((lp = (struct vmgr_tape_dgn *) vmgr_listweight (flags, &list)) != NULL) {
			listentry (lp->dgn, lp->weight, lp->deltaweight);
			flags = VMGR_LIST_CONTINUE;
		}
		(void) vmgr_listweight (VMGR_LIST_END, &list);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}

print_header(void)
{
	printf ("%-10s %-10s %s\n", "DGN", "WEIGHT", "DELTA");
}

listentry(dgn, weight, delta)
char *dgn;
int weight;
int delta;
{
	printf ("%-10s %-10i %i\n", dgn, weight, delta);
}
