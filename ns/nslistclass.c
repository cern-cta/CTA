/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nslistclass.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:22 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	nslistclass - query a given class or list all existing file classes */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cgetopt.h"
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
int nohdr = 0;
main(argc, argv)
int argc;
char **argv;
{
	int c;
	int classid = 0;
	char *class_name = NULL;
	struct Cns_fileclass Cns_fileclass;
	char *dp;
	int errflg = 0;
	int flags;
	Cns_list list;
	static struct Coptions longopts[] = {
		{"id", REQUIRED_ARGUMENT, 0, OPT_CLASS_ID},
		{"name", REQUIRED_ARGUMENT, 0, OPT_CLASS_NAME},
		{"nohdr", NO_ARGUMENT, &nohdr, 1},
		{0, 0, 0, 0}
	};
	struct Cns_fileclass *lp;
	char *server = NULL;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "h:", longopts, NULL)) != EOF) {
		switch (c) {
		case OPT_CLASS_ID:
			if ((classid = strtol (Coptarg, &dp, 10)) <= 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid classid %s\n", Coptarg);
				errflg++;
			}
			break;
		case OPT_CLASS_NAME:
			if (strlen (Coptarg) > CA_MAXCLASNAMELEN) {
				fprintf (stderr,
				    "class name too long: %s\n", Coptarg);
				errflg++;
			} else
				class_name = Coptarg;
			break;
		case 'h':
			server = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (Coptind < argc)
		errflg++;
	if (errflg) {
		fprintf (stderr, "usage: %s %s", argv[0],
		    "--id classid --name class_name [-h name_server] [--nohdr]\n");
		exit (USERR);
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	if (classid > 0 || class_name) {
		if (Cns_queryclass (server, classid, class_name, &Cns_fileclass) < 0) {
			char buf[256];
			if (classid) sprintf (buf, "%d", classid);
			if (class_name) {
				if (classid) strcat (buf, ", ");
				else buf[0] = '\0';
				strcat (buf, class_name);
			}
			fprintf (stderr, "nslistclass %s: %s\n", buf,
			    (serrno == ENOENT) ? "No such class" : sstrerror(serrno));
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (USERR);
		}
		listentry (&Cns_fileclass);
		if (Cns_fileclass.tppools)
			free (Cns_fileclass.tppools);
	} else {
		flags = CNS_LIST_BEGIN;
		while ((lp = Cns_listclass (server, flags, &list)) != NULL) {
			if (flags == CNS_LIST_CONTINUE && ! nohdr)
				printf ("\n");
			listentry (lp);
			flags = CNS_LIST_CONTINUE;
		}
		(void) Cns_listclass (server, CNS_LIST_END, &list);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}

listentry(Cns_fileclass)
struct Cns_fileclass *Cns_fileclass;
{
	struct group *gr;
	int i;
	char *p;
	struct passwd *pw;
	char retenpstr[20];
	static gid_t sav_gid = -1;
	static char sav_gidstr[7];
	static uid_t sav_uid = -1;
	static char sav_uidstr[CA_MAXUSRNAMELEN+1];

	if (Cns_fileclass->uid != sav_uid) {
		sav_uid = Cns_fileclass->uid;
		if (sav_uid == 0)
			strcpy (sav_uidstr, "-");
		else if (pw = getpwuid (sav_uid))
			strcpy (sav_uidstr, pw->pw_name);
		else
			sprintf (sav_uidstr, "%-8u", sav_uid);
	}
	if (Cns_fileclass->gid != sav_gid) {
		sav_gid = Cns_fileclass->gid;
		if (sav_gid == 0)
			strcpy (sav_gidstr, "-");
		else if (gr = getgrgid (sav_gid))
			strcpy (sav_gidstr, gr->gr_name);
		else
			sprintf (sav_gidstr, "%-6u", sav_gid);
	}
	if (Cns_fileclass->retenp_on_disk == AS_LONG_AS_POSSIBLE)
		strcpy (retenpstr, "AS_LONG_AS_POSSIBLE");
	else if (Cns_fileclass->retenp_on_disk == INFINITE_LIFETIME)
		strcpy (retenpstr, "INFINITE_LIFETIME");
	else
		sprintf (retenpstr, "%d", Cns_fileclass->retenp_on_disk);
	p = Cns_fileclass->tppools;
	if (nohdr) {
		printf ("%d %s %-8.8s %-6.6s 0x%x %d %d %d %d %d %d %d %s %s",
		    Cns_fileclass->classid, Cns_fileclass->name, sav_uidstr, sav_gidstr,
		    Cns_fileclass->flags, Cns_fileclass->maxdrives,
		    Cns_fileclass->min_filesize, Cns_fileclass->max_filesize,
		    Cns_fileclass->max_segsize, Cns_fileclass->migr_time_interval,
		    Cns_fileclass->mintime_beforemigr, Cns_fileclass->nbcopies,
		    retenpstr, p);
	} else {
		printf ("CLASS_ID	%d\n", Cns_fileclass->classid);
		printf ("CLASS_NAME	%s\n", Cns_fileclass->name);
		printf ("CLASS_UID	%s\n", sav_uidstr);
		printf ("CLASS_GID	%s\n", sav_gidstr);
		printf ("FLAGS		0x%x\n", Cns_fileclass->flags);
		printf ("MAXDRIVES	%d\n", Cns_fileclass->maxdrives);
		printf ("MIN FILESIZE	%d\n", Cns_fileclass->min_filesize);
		printf ("MAX FILESIZE	%d\n", Cns_fileclass->max_filesize);
		printf ("MAX SEGSIZE	%d\n", Cns_fileclass->max_segsize);
		printf ("MIGR INTERVAL	%d\n", Cns_fileclass->migr_time_interval);
		printf ("MIN TIME	%d\n", Cns_fileclass->mintime_beforemigr);
		printf ("NBCOPIES	%d\n", Cns_fileclass->nbcopies);
		printf ("RETENP_ON_DISK	%s\n", retenpstr);
		printf ("TAPE POOLS	%s", Cns_fileclass->nbtppools ? p : "");
	}
	for (i = 1; i < Cns_fileclass->nbtppools; i++) {
		p += (CA_MAXPOOLNAMELEN+1);
		printf (":%s", p);
	}
	printf ("\n");
	return (0);
}
