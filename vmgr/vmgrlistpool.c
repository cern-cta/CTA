/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrlistpool - query a given pool or list all existing tape pools */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_api.h"

extern	char	*optarg;
extern	int	optind;

void listentry(char *pool_name,
               uid_t pool_uid,
               gid_t pool_gid,
               u_signed64 capacity,
               u_signed64 tot_free_space,
               int sflag)
{
	signed64 c64;
	signed64 f64;
	struct group *gr;
	struct passwd *pw;
	static gid_t sav_gid = -1;
	static char sav_gidstr[7];
	static uid_t sav_uid = -1;
	static char sav_uidstr[CA_MAXUSRNAMELEN+1];
	char tmpbuf[80];
	char tmpbuf2[80];

	if (pool_uid != sav_uid) {
		sav_uid = pool_uid;
		if (pool_uid == 0)
			strcpy (sav_uidstr, "-");
		else if ((pw = getpwuid (sav_uid)))
			strcpy (sav_uidstr, pw->pw_name);
		else
			sprintf (sav_uidstr, "%-*u", sflag ? 0 : 8, sav_uid);
	}
	if (pool_gid != sav_gid) {
		sav_gid = pool_gid;
		if (pool_gid == 0)
			strcpy (sav_gidstr, "-");
		else if ((gr = getgrgid (sav_gid)))
			strcpy (sav_gidstr, gr->gr_name);
		else
			sprintf (sav_gidstr, "%-*u", sflag ? 0 : 6, sav_gid);
	}
	c64 = capacity;		/* because C compiler on Windows/NT */
	f64 = tot_free_space;	/* cannot cast u_signed64 to double */
	if (sflag)
		printf ("%s %s %s %s %s %.1f\n",
			pool_name, sav_uidstr, sav_gidstr,
			u64tostr(capacity, tmpbuf, 0), 
			u64tostr(tot_free_space, tmpbuf2, 0), capacity ?
			(double)f64 * 100. / (double)c64 : 0);
	else
		printf ("%-15s %-8.8s %-6.6s CAPACITY %sB FREE %sB (%5.1f%%)\n",
			pool_name, sav_uidstr, sav_gidstr,
			u64tostru (capacity, tmpbuf, 9),
			u64tostru (tot_free_space, tmpbuf2, 9), capacity ?
			(double)f64 * 100. / (double)c64 : 0);
}

int main(int argc,
         char **argv)
{
	int c;
	u_signed64 capacity;
	int errflg = 0;
	int sflag = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_pool_byte_u64 *lp = NULL;
	gid_t pool_gid = 0;
	char *pool_name = NULL;
	uid_t pool_uid = 0;
	u_signed64 tot_free_space;

        while ((c = getopt (argc, argv, "P:s")) != EOF) {
                switch (c) {
		case 'P':
			pool_name = optarg;
                        break;
		case 's':
			sflag = 1;
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
                fprintf (stderr, "usage: %s %s", argv[0], "[-P pool_name] [-s]\n");
                exit (USERR);
        }
 
	if (pool_name) {
		if (vmgr_querypool (pool_name, &pool_uid, &pool_gid, &capacity,
		    &tot_free_space) < 0) {
			fprintf (stderr, "vmgrlistpool %s: %s\n", pool_name,
				 (serrno == ENOENT) ? "No such pool" : sstrerror(serrno));
			exit (USERR);
		}
		listentry (pool_name, pool_uid, pool_gid, capacity, tot_free_space, sflag);
	} else {
		flags = VMGR_LIST_BEGIN;
		while ((lp = vmgr_listpool_byte_u64 (flags, &list)) != NULL) {
			listentry (lp->name, lp->uid, lp->gid,
			lp->capacity_byte_u64, lp->tot_free_space_byte_u64,
			sflag);
			flags = VMGR_LIST_CONTINUE;
		}
		(void) vmgr_listpool_byte_u64 (VMGR_LIST_END, &list);
	}
	exit (0);
}
