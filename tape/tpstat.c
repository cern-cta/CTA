/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: tpstat.c,v $ $Revision: 1.6 $ $Date: 2007/03/01 16:41:37 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	tpstat - tape status display */
#include <stdio.h>
#include <stdlib.h>
#include <pwd.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Ctape_api.h"

void usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s [hostname]\n", cmd);
}

int main(argc, argv)
int	argc;
char	**argv;
{
	int c, i;
	struct drv_status drv_status[CA_MAXNBDRIVES];
	char fulldrivename[18];
	char hostname[CA_MAXHOSTNAMELEN+1];
	char jobid[11];
	char label;
	char mounted;
	char name[CA_MAXUSRNAMELEN+1];
	int nbentries = CA_MAXNBDRIVES;
	char *p;
	struct passwd *pwd;
	char ring;
	char status[5];
	struct tm *tm;
	char timestamp[9];

	if (argc > 2) {
		usage (argv[0]);
		exit (USERR);
	}
	if (argc == 2)
		strcpy (hostname, argv[1]);
	else
		gethostname (hostname, sizeof(hostname));
	c = Ctape_status (hostname, drv_status, nbentries);
	if (c > 0) {
		if ((p = strchr (hostname, '.'))) *p = '\0';
		printf ("userid     jid  dgn        stat dvn                 rl  vsn    vid\n");
		for (i = 0; i < c; i++) {
			if (drv_status[i].asn == 0) {
				if (drv_status[i].status == 0)
					strcpy (status, "down");
				else
					strcpy (status, "idle");
				name[0] = '\0';
				jobid[0] = '\0';
				ring = ' ';
				label = ' ';
				mounted = ' ';
				timestamp[0] = '\0';
			} else {
				if (drv_status[i].status <= 0)
					strcpy (status, "wdwn");
				else
					strcpy (status, "assn");
				if ((pwd = getpwuid (drv_status[i].uid)) == NULL) {
					sprintf (name, "%d", drv_status[i].uid);
				} else {
					strcpy (name, pwd->pw_name);
				}
				sprintf (jobid, "%d", drv_status[i].jid);
				if (drv_status[i].mode == WRITE_DISABLE)
					ring = 'o';
				else
					ring = 'i';
				label = *drv_status[i].lbltype;
				if (drv_status[i].tobemounted)
					mounted = '*';
				else
					mounted = ' ';
				tm = localtime (&drv_status[i].asn_time);
				sprintf (timestamp,"%02d:%02d:%02d",
					tm->tm_hour, tm->tm_min, tm->tm_sec);
			}
			sprintf (fulldrivename, "%s@%-8s", drv_status[i].drive,
				hostname);
			printf ("%-8s %-6s %-8s   %-4s %-17s   %c%c%c%-6s %-6s %s\n",
				name, jobid, drv_status[i].dgn, status, fulldrivename,
				ring, label, mounted, drv_status[i].vsn,
				drv_status[i].vid, timestamp);
		}
	}
	exit (c > 0 ? 0 : SYERR);
}

