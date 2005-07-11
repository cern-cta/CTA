/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: reclaim.c,v $ $Revision: 1.6 $ $Date: 2005/07/11 11:46:03 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*      reclaim - reset information concerning a volume */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cns_api.h"
#include "serrno.h"
#include "vmgr_api.h"
extern	char	*getconfent();
extern	char	*optarg;
extern	int	optind;
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char *Cns_hosts = NULL;
	FILE *df;
	struct Cns_direntape *dtp;
	int errflg = 0;
	int flags;
	char *host = NULL;
	Cns_list list;
	char *p;
	char p_stat[9];
	char path[CA_MAXPATHLEN+1];
	struct vmgr_tape_info tape_info;
	FILE *tmpfile();
	char *vid = NULL;

	while ((c = getopt (argc, argv, "h:V:")) != EOF) {
		switch (c) {
		case 'h':
			host = optarg;
			break;
		case 'V':
			vid = optarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (optind < argc || ! vid) {
		errflg++;
	}
	if (errflg) {
		fprintf (stderr, "usage: %s [-h name_server] -V vid\n", argv[0]);
		exit (USERR);
	}

	/* check if the volume is FULL */

	if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
		fprintf (stderr, "reclaim %s: %s\n", vid,
		    (serrno == ENOENT) ? "No such volume" : sstrerror(serrno));
		exit (USERR);
	}
	if ((tape_info.status & TAPE_FULL) == 0) {
		if (tape_info.status == 0) strcpy (p_stat, "FREE");
		else if (tape_info.status & TAPE_BUSY) strcpy (p_stat, "BUSY");
		else if (tape_info.status & TAPE_RDONLY) strcpy (p_stat, "RDONLY");
		else if (tape_info.status & EXPORTED) strcpy (p_stat, "EXPORTED");
		else if (tape_info.status & DISABLED) strcpy (p_stat, "DISABLED");
		else strcpy (p_stat, "?");
		fprintf (stderr, "Volume %s has status %s\n", vid, p_stat);
		exit (USERR);
	}

	/* check if the volume still contains active files */

	if ((df = tmpfile ()) == NULL) {
		fprintf (stderr, "Cannot create temporary file\n");
		exit (USERR);
	}

	if (! host && (Cns_hosts = getconfent ("CNS", "HOST", 1))) {
		Cns_hosts = strdup (Cns_hosts);
		host = Cns_hosts;
		if (p = strpbrk (Cns_hosts, " \t\n")) {
			*p = '\0';
			Cns_hosts = p + 1;
		} else
			Cns_hosts = NULL;
	}
	while (1) {
		flags = CNS_LIST_BEGIN;
		while ((dtp = Cns_listtape (host, vid, flags, &list)) != NULL) {
			flags = CNS_LIST_CONTINUE;
			if (dtp->s_status == 'D') {
				if (Cns_getpath (host, dtp->parent_fileid, path) < 0) {
					fprintf (stderr, "%s\n", sstrerror(serrno));
					exit (USERR);
				}
				fprintf (df, "%s\n", path);
			} else {
				fprintf (stderr,
				    "Volume %s still contains active files\n", vid);
				exit (USERR);
			}
		}
		if (serrno > 0 && serrno != ENOENT) {
			fprintf (stderr, "reclaim %s: %s\n", vid,
			    sstrerror(serrno));
			exit (SYERR);
		}
		(void) Cns_listtape (host, vid, CNS_LIST_END, &list);
		if (! Cns_hosts) break;
		host = Cns_hosts;
		if (p = strpbrk (Cns_hosts, " \t\n")) {
			*p = '\0';
			Cns_hosts = p + 1;
		} else
			Cns_hosts = NULL;
	}

	/* remove logically deleted files */

	rewind (df);
	while (fgets (path, CA_MAXPATHLEN, df)) {
		*(path + strlen (path)) = '\0';
		if (Cns_unlink (path)) {
			fprintf (stderr, "Cannot delete %s: %s\n", path,
			    sstrerror (serrno));
			exit (USERR);
		}
	}
	fclose (df);

	/* reset nbfiles, estimated_free_space and status for this volume */

	if (vmgr_reclaim (vid)) {
		fprintf (stderr, "reclaim %s: %s\n", vid,
		    (serrno == ENOENT) ? "No such volume" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
