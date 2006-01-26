/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nschmod.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:21 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	nschmod - change directory/file permissions in name server */
#include <errno.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
extern	char	*getenv();
main(argc, argv)
int argc;
char **argv;
{
	int absmode = 0;
	int c;
	char *dp;
	int errflg = 0;
	char fullpath[CA_MAXPATHLEN+1];
	int i;
	char *mode_arg;
	mode_t newmode;
	char *p;
	char *path;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	if (argc < 3) {
		fprintf (stderr,
		    "usage: %s mode file...\n", argv[0]);
		exit (USERR);
	}
	mode_arg = argv[1];
	if (isdigit (*mode_arg)) {	/* absolute mode */
		newmode = strtol (mode_arg, &dp, 8);
		if (*dp != '\0') {
			fprintf (stderr, "invalid mode: %s\n", mode_arg);
			exit (USERR);
		}
		absmode = 1;
	} else {	/* mnemonic */
		fprintf (stderr, "symbolic modes not supported yet\n");
		exit (USERR);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	for (i = 2; i < argc; i++) {
		path = argv[i];
		if (*path != '/' && strstr (path, ":/") == NULL) {
			if ((p = getenv (CNS_HOME_ENV)) == NULL ||
			    strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
				fprintf (stderr, "%s: invalid path\n", path);
				errflg++;
				continue;
			} else
				sprintf (fullpath, "%s/%s", p, path);
		} else {
			if (strlen (path) > CA_MAXPATHLEN) {
				fprintf (stderr, "%s: %s\n", path,
				    sstrerror(SENAMETOOLONG));
				errflg++;
				continue;
			} else
				strcpy (fullpath, path);
		}
		if (Cns_chmod (fullpath, newmode)) {
			fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
			errflg++;
		}
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	if (errflg)
		exit (USERR);
	exit (0);
}
