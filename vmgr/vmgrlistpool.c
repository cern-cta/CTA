/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrlistpool.c,v $ $Revision: 1.6 $ $Date: 2000/08/22 13:13:22 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrlistpool - query a given pool or list all existing tape pools */
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
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_pool *lp;
	gid_t pool_gid = 0;
	char *pool_name = NULL;
	uid_t pool_uid = 0;
	u_signed64 tot_free_space;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

        while ((c = getopt (argc, argv, "P:")) != EOF) {
                switch (c) {
		case 'P':
			pool_name = optarg;
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
                fprintf (stderr, "usage: %s %s", argv[0], "[-P pool_name]\n");
                exit (USERR);
        }
 
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, VMG52);
		exit (SYERR);
	}
#endif
	if (pool_name) {
		if (vmgr_querypool (pool_name, &pool_uid, &pool_gid,
		    &tot_free_space) < 0) {
			fprintf (stderr, "vmgrlistpool %s: %s\n", pool_name,
			    (serrno == ENOENT) ? "No such pool" : sstrerror(serrno));
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (USERR);
		}
		listentry (pool_name, pool_uid, pool_gid, tot_free_space);
	} else {
		flags = VMGR_LIST_BEGIN;
		while ((lp = vmgr_listpool (flags, &list)) != NULL) {
			listentry (lp->name, lp->uid, lp->gid, lp->tot_free_space);
			flags = VMGR_LIST_CONTINUE;
		}
		(void) vmgr_listpool (VMGR_LIST_END, &list);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}

listentry(pool_name, pool_uid, pool_gid, tot_free_space)
char *pool_name;
uid_t pool_uid;
gid_t pool_gid;
u_signed64 tot_free_space;
{
	struct group *gr;
	struct passwd *pw;
	static gid_t sav_gid = -1;
	static char sav_gidstr[7];
	static uid_t sav_uid = -1;
	static char sav_uidstr[CA_MAXUSRNAMELEN+1];
	char tmpbuf[8];

	if (pool_uid != sav_uid) {
		sav_uid = pool_uid;
		if (pool_uid == 0)
			strcpy (sav_uidstr, "-");
		else if (pw = getpwuid (sav_uid))
			strcpy (sav_uidstr, pw->pw_name);
		else
			sprintf (sav_uidstr, "%-8u", sav_uid);
	}
	if (pool_gid != sav_gid) {
		sav_gid = pool_gid;
		if (pool_gid == 0)
			strcpy (sav_gidstr, "-");
		else if (gr = getgrgid (sav_gid))
			strcpy (sav_gidstr, gr->gr_name);
		else
			sprintf (sav_gidstr, "%-6u", sav_gid);
	}
	printf ("%-15s %-8.8s %-6.6s %sB\n", pool_name, sav_uidstr, sav_gidstr,
	    u64tostru (tot_free_space, tmpbuf, 7));
}
