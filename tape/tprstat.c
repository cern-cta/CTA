/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: tprstat.c,v $ $Revision: 1.2 $ $Date: 2007/03/01 16:41:37 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	tprstat - resource reservation status display */
#include <stdio.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Ctape_api.h"
#define MAXDGP 4
#define MAXJOBS 64

void usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s [hostname]\n", cmd);
}

int main(argc, argv)
int	argc;
char	**argv;
{
	int c, i, j;
	struct rsv_status rsv_status[MAXJOBS];
	char hostname[CA_MAXHOSTNAMELEN+1];
	char jobid[11];
	char name[CA_MAXUSRNAMELEN+1];
	int nbdgp = MAXDGP;
	int nbentries = MAXJOBS;
	struct passwd *pwd;

	if (argc > 2) {
		usage (argv[0]);
		exit (USERR);
	}
	if (argc == 2)
		strcpy (hostname, argv[1]);
	else
		gethostname (hostname, sizeof(hostname));

	/* allocate dgn_rsv_status structures */

	for (i = 0; i < MAXJOBS; i++) {
		if ((rsv_status[i].dg = calloc (MAXDGP, sizeof(struct dgn_rsv_status))) == NULL) {
			perror ("tprstat");
			exit (SYERR);
		}
	}

	c = Ctape_rstatus (hostname, rsv_status, nbentries, nbdgp);
	if (c > 0) {
		printf ("userid      jid dgn        rsvd used\n");
		for (i = 0; i < c; i++) {
			if (i == 0)
				strcpy (name, "tpdaemon");
			else {
				if ((pwd = getpwuid (rsv_status[i].uid)) == NULL) {
					sprintf (name, "%d", rsv_status[i].uid);
				} else {
					strcpy (name, pwd->pw_name);
				}
			}
			sprintf (jobid, "%d", rsv_status[i].jid);
			for (j = 0; j < rsv_status[i].count; j++) {
				if (j) {
					name[0] = '\0';
					jobid[0] = '\0';
				}
				printf ("%-8s %6s %-8s   %4d %4d\n",
				    name, jobid, rsv_status[i].dg[j].name,
				    rsv_status[i].dg[j].rsvd,
				    rsv_status[i].dg[j].used);
			}
		}
	}
	exit (c > 0 ? 0 : SYERR);
}

